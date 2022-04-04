### Unique Last Name
____

Following Assumptions are made about the script and data

1. S3 object will be instantiated externally using the secret. 
2. customer_name: will have `space` delimitted words and order will be:

    [FirstName]  [MiddleName]<(optional)>  [LastName]
   
3. User running the code has write access to `Network Drive`.


   
### Logic:
___

1. Download the transaction file from s3 bucket when credentials are provided. 
2. store the csv file locally and start processing it. 
3. read each line and and for column `customer name` check if the last name,
   is already present in dictionary, if not add.
4. After reading the file write this file to Network drive. 


