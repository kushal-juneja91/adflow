class FileSaveUtil():
    def write_data(self,df, filename_with_location, filesystem=None, type='json'):
        df=df.coalesce(1)
        if filesystem=='local':
            filename_with_location = "file://" + filename_with_location
        if type=='csv':
            try:
                df.write.csv(filename_with_location)
                print("file written:::" + filename_with_location)
            except:
                print("error writing csv")
        elif type=='json':
            try:
                df.write.json(filename_with_location)
                print("file written:::" + filename_with_location)
            except:
                print("error writing json")
        else:
            print("please provide a output format")


