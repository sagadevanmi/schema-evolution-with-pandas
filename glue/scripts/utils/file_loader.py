import os
import definitions


class FileLoader:
    """
    Class containing function to return path
    """

    @staticmethod
    def load_data(path_list: list) -> str:
        """Function that returns path of file from root directory

        Args:
            path_list (list): list containing sub directories and file name

        Returns:
            test_folder_root: path to specified file in path_list from root
        """
        # test_folder_root = os.path.dirname(os.path.abspath(__file__))
        test_folder_root = definitions.ROOT_DIR

        for element in path_list:
            test_folder_root = os.path.join(test_folder_root, element)
        return test_folder_root

    @staticmethod
    def load_data_s3(path_list: list) -> str:
        """Function that returns path of file from root directory

        Args:
            path_list (list): list containing sub directories and file name

        Returns:
            folder_root: path to specified file in path_list from root
        """
        folder_root = "s3://"
        for element in path_list:
            folder_root = folder_root + element + "/"
        return folder_root