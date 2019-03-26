class FileSaveUtil():
    def write_to_csv(self,df, filename_with_location):
        try:
            df.write.csv(filename_with_location)
            print("file written:::" + filename_with_location)
        except:
            print("error writing")

