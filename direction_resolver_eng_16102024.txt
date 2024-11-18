import pandas as pd
from datetime import timedelta

def check_delimiter(file) -> str:
    """
    Determines the delimiter of the inputted csv-file. 

    Parameters:
        file -- inputted csv-file (road image data)
    
    Returns:
        ',' -- comma delimiter
        ';' -- semicolon delimiter 
    """
    with open(file, 'r') as file:
        firstline = file.readline()
        if firstline.count(',') > firstline.count(';'):
            return ','
        else: 
            return ';'
    
def check_dupes(df) -> bool:
    dupes = df.duplicated(subset = ['kuvatieto_id'], keep = 'first')
    dupe_rows = df[dupes]
    if dupe_rows.shape[0] == 0:
        return False
    return True

def extract_data(file) -> pd.DataFrame:
    """
    Transforms the inputted csv-file into a more processable pandas dataframe; 
    removes unnecessary columns, and makes necessary changes to attribute values.

    Parameters: 
        file -- File path as a string, with which the desired file is retrieved locally.
                Note: use 2 backslashes ('\\') to avoid them being read as escapes. 
    
    Returns:
        df2 -- modified dataframe with the desired contents. 
    """
    delim = check_delimiter(file)
    df = pd.read_csv(file, delimiter = delim)

    selected = ['kuvatieto_id', 'tie', 'tieosa', 'etaisyys','suunta_tieosoite','kuvaussuunta', 'kuvausaika', 'nodirection']
    df2 = df[selected]
    if check_dupes(df2):
        return 'Error! Duplicate(s) found'
     
    df2['kuvausaika'] = pd.to_datetime(df2['kuvausaika'])
    
    #changing suunta_tieosoite values to -99, so that they stand out once changes are made later
    df2['suunta_tieosoite'] = -999
    
    return df2

def reorder_df(df) -> pd.DataFrame:
    pd.set_option('display.max_rows', 600)
    return df.sort_values(by = ['tie', 'tieosa', 'etaisyys'])

def degree_diff(angle1, angle2):
    """
    Calculates the difference between two angles in degrees. 

    Parameters:
        angle1 -- the angle of the first image in degrees 
        angle2 -- the angle of the second/reference image in degrees 
    
    Returns: 
        The difference between the two angles as a float in the range 0-180
    """
    if angle2 is not None:
        difference = abs(angle1 - angle2)
        return min(difference, 360 - difference)

def can_append_to_list(image, last_image) -> bool:
    """
    Determines whether an image can be added to an image series.

    Parameters: 
        image -- list, image (and data) currently being examined in an iteration of the for-loop
        last_image -- list, reference image (and data) retried from the "last_images" list. 
    
    Returns:
        True -- the conditions are met, image can be added
        False -- one or multiple conditions not met
    """
    new_distance, new_angle, new_time = image[0], image[2], image[3]
    last_distance, last_angle, last_time = last_image[0], last_image[2], last_image[3]
    distance_diff = abs(new_distance - last_distance)
    angle_diff = degree_diff(new_angle, last_angle)
    time_diff = abs(new_time - last_time)

    # Conditíons for belonnging to the same image series. 
    # Increasing the distance limit value can have a positive effect on series length.
    return distance_diff <= 70 and angle_diff <= 40 and time_diff <= timedelta(minutes=1)

def add_to_list(image, index: int, sections, last_images: list, tieosa):
    """
    Adds the image to a fitting image series once it has been determined with
    the can_append_to_list function. 

    Parametrit:
        image -- list, image (and data) currently being examined in an iteration of the for-loop
        index -- index, integer 
        sections -- the inner of the nested dictionary "road_images". The keys are road sections, 
                    and their values are lists of image series.
        last_images -- a list of the last image in every series created in the for-loop thus far 
        tieosa -- the section of road on which the image currently being processed was taken 

    Returns:
        None
    """
    sections[tieosa][index].append(image)
    last_images[index] = image 

def create_new_series(image, sections, last_images: list, tieosa): 
    """
    Creates a new image series in the event that none of the images in the last_images list are fitting. 
    Also appends the image to last_images

    Parametrit:
        image -- list, image (and data) currently being examined in an iteration of the for-loop
        sections -- the inner of the nested dictionary "road_images". The keys are road sections, 
                    and their values are lists of image series.
        last_images -- a list of the last image in every series created in the for-loop thus far 
        tieosa -- the section of road on which the image currently being processed was taken

    Returns:
        None
    """
    if tieosa not in sections:
        sections[tieosa] = []
        
    sections[tieosa].append([image])
    last_images.append(image)

def build_dict(df) -> dict:
    """
    Constructs nested dictionaries and gives the image data a 
    more approachable and legible structure, organized by road 
    network address. Image series are on the lowest level as a 
    list of lists containing data about individual images that 
    have been separated by time, angle, and location along the 
    road network. During the series creation phase, series are 
    broken if the difference in time, angle or distance between
    two adjacent images crosses the given limit value.  

    The inner dictionary containing road sections as keys also
    has a key named 'last_images' that contains the last image 
    of each image series created thus far. 

    Parameters:
        df -- reordered datafame (created with the reorder_df function) 

    Returns:
        road_images -- nested dictionary with the following structure. 
                    {
                        tie: [
                                {
                                    tieosa: [
                                        [
                                        [etäisyys, kuvatieto_id, suunta, aika, suunta_tieosoite, nodirection],
                                        ...
                                        ],
                                        ...
                                    ],
                                    'last_images': [

                                        [etäisyys, kuvatieto_id, suunta, aika, suunta_tieosoite],
                                        ...
                                    ]
                                },
                            ...
                            ],
                        ...
                    }

    """
    road_images = {}

    for index, row in df.iterrows():
        tie = row['tie']
        tieosa = row['tieosa']
        etäisyys = row['etaisyys']
        kuvatieto_id = row['kuvatieto_id']
        suunta = row['kuvaussuunta']
        aika = row['kuvausaika']
        suunta_tieosoite = row['suunta_tieosoite']
        no_direction = row['nodirection']

        image = [etäisyys, kuvatieto_id, suunta, aika, suunta_tieosoite, no_direction]

        if tie not in road_images:
            road_images[tie] = []
        
        tieosa_olemassa  =  False
        
        for sections in road_images[tie]:
            if tieosa in sections:
                tieosa_olemassa = True
                last_images = sections.get('last_images', [])
                uusi_sarja = True 

                for i, last_image in enumerate(last_images):
                    if can_append_to_list(image, last_image):
                        add_to_list(image, i, sections, last_images, tieosa)
                        uusi_sarja = False 
                        break 
                
                if uusi_sarja:
                    create_new_series(image, sections, last_images, tieosa)
                
                sections['last_images'] = last_images 
                break 

        if not tieosa_olemassa:
            road_images[tie].append({tieosa: [[[etäisyys, kuvatieto_id, suunta, aika, suunta_tieosoite, no_direction]]],'last_images': [[etäisyys, kuvatieto_id, suunta, aika, suunta_tieosoite]]})
    
    return road_images 

def nice_print(dict):
    """
    Prints the contents of the nested dictionary in a legible way. 

    Parameters:
        dict -- nested dictionary built with the build_dict function 

    Returns:
        None (print)

    """
    for tie, sections in dict.items():
        print(f"Road number: {tie}")
        for section in sections:
            for tieosa, datalist in section.items():
                if tieosa == 'last_images':
                    continue
                print(f"  Road section: {tieosa}")
                for series in datalist:
                    for image in series:
                        print(f"    Kuvatieto_id: {image[1]} | Distance: {image[0]:<4} | Angle: {image[2]:>5} ° | Time: {image[3]} | suunta_tieosoite: {image[4]} | No direction: {image[5]}")
                    print("\n")
        print(f"{40*"- "}\n")

def define_direction(dict):
    """
    Determines the direction (suunta_tieosoite) for image series. 
    
    Parameters: 
        dict -- nested dictionary built with the build_dict function
    
    Returns:
        dict -- nested dictionary in which the values for the suunta_tieosoite 
                attribute have been changed as follows:
                1 = rising direction 
                2 = falling direction 
                -999 = undetermined

                The boolean value in the nodirection column is also changed to False
                if a new direction is assigned. 
    """
    for tie, sections in dict.items():
            for section in sections:
                for tieosa, datalist in section.items():
                    if tieosa == 'last_images':
                        continue
                    for series in datalist:
                        time_differnece = series[0][3] - series[-1][3]
                        if len(series)>1 and time_differnece!=timedelta(seconds=0):
                            if time_differnece < timedelta(seconds=0):
                                for kuva in series:
                                    kuva[4] = 1
                                    kuva[5] = False
                            elif time_differnece > timedelta(seconds=0):
                                for kuva in series:
                                    kuva[4] = 2
                                    kuva[5] = False
    return dict

def dict_to_df(dict) -> pd.DataFrame:
    images = []
    for tie, sections in dict.items():
            for section in sections:
                for tieosa, datalist in section.items():
                    if tieosa == 'last_images':
                         continue
                    for series in datalist:
                        for image in series:
                            images.append({'kuvatieto_id': image[1], 'tie': image, 'tieosa': tieosa, 
                                          'etaisyys':image[0], 'kuvausaika': image[3], 'suunta_tieosoite': image[4], 'nodirection': image[5]})
    df = pd.DataFrame(images)
    
    pd.set_option('display.max_rows', 600)
    return df

def changed_stats(df: pd.DataFrame):
    """
    Calculates brief statistics based on a dataframe 
    with the changes made with the define_direction function. 

    Parameters: 
        df -- dataframe made with the dict_to_df function 

    Returns:
        None (print)
    """
    rows = len(df)
    unchanged = df['suunta_tieosoite'].value_counts().get(-999, 0)
    ones = df['suunta_tieosoite'].value_counts().get(1, 0)
    twos = df['suunta_tieosoite'].value_counts().get(2, 0)
    
    header1 = "Updated:"
    subheader1 = "   Ones (rising)" 
    subheader2 = "   Twos (falling)"
    header2 = "Unchanged:" 
    header3 = "TOTAL:"

    print(f"\nCompleted changes:\n")
    print(f"{header1:<25}|{(rows-unchanged):>10}")
    print(f"{subheader1:<25}|{ones:>10}")
    print(f"{subheader2:<25}|{twos:>10}")
    print(f"{header2:<25}|{unchanged:>10}")
    print(f"{header3:<25}|{rows:>10}\n")
    print(f"Of the inputted images, {((rows-unchanged)/rows):.1%} of them were updated.\n")

def write_fixed_csv(filepath: str, destination_path: str, df: pd.DataFrame):
    """
    Writes a dataframe into a csv-file to the user's desired destination.
    The new csv-file is named after the originally inputted file.

    Parameters:
        filepath -- the path of the original csv-file 
        destination_path -- the path of the folder where the new file will be saved 
        df -- dataframe to be written into a new csv-file  

    Returns:
        None, the function prints a confirmation message when the file has been successfully saved. 
    """
    filepath_parts = filepath.split('\\')
    filename_updated = f'{filepath_parts[-1][:-4]}_UPDATED.csv'
    filename_nodirection = f'{filepath_parts[-1][:-4]}_NODIRECTION.csv'
    updated_destination = destination_path + filename_updated 
    nodir_destination = destination_path + filename_nodirection 

    df.loc[df['nodirection'] == False].to_csv(updated_destination, index = False)
    df.loc[df['nodirection'] == True].to_csv(nodir_destination, index = False)

    confirmation = f"********* The updated csv-file was successfully saved: {updated_destination}. *********"

    changed_stats(df)
    
    print(f"{len(confirmation)*"*"}\n\n{confirmation}\n\n{len(confirmation)*"*"}")

if __name__ == "__main__":
    filepath = "file\\path.csv"
    destination = "folder\\path\\"

    for_processing = build_dict(reorder_df(extract_data(filepath)))

    fixed_dict = define_direction(for_processing)

    # print completed changes before writing the csv-file or comment out to skip 
    nice_print(fixed_dict)

    fixed_df = dict_to_df(fixed_dict)

    write_fixed_csv(filepath, destination, fixed_df)