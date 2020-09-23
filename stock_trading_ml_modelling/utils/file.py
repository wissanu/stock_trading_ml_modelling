import os

def replace_file(old_file:str,new_file:str):
    """Function to delete an old file nad then replace it with aother one
    
    args:
    ------
    old_file - str - filepath for the file to be removed
    new_file - str - filepath for the fiel to replace the old file

    returns:
    ------
    True if successful, exception thrown if not 
    """
    try:
        os.remove(old_file)
        print('\nSUCCESSFULLY REMOVED {}'.format(old_file))
    except Exception as e:
        print('\WARNING - COULD NOT REMOVE {}:{}'.format(old_file,e))
        raise Warning(e)
    try:
        os.rename(new_file,old_file)
        print('\nSUCCESSFULLY RENAMED {} TO {}'.format(new_file,old_file))
    except Exception as e:
        print('\nERROR - RENAMING:{}'.format(e))
        raise Exception(e)
    return True