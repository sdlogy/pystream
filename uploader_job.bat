dt.exe /s:SQL /s.ConnectionString:"Data Source=localhost;Initial Catalog=stackOverflow;Trusted_Connection=yes" /s.Query:"SELECT * FROM Users" /t:DocumentDB /t.ConnectionString:AccountEndpoint=https://commdoc.documents.azure.com:443/;AccountKey=mQggCSLkspr10NKU4tDQYUI7S6fdvcOHLR4yGFld3qQlW8qYunbdFGMyjJ1001e4xxKt7Kg8NdfYCzduX0ytUQ==;Database=tripdata /t.IdField:Id /t.DisableIdGeneration /t.UpdateExisting /t.Collection:trips