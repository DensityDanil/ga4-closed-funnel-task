-- https://docs.google.com/spreadsheets/d/1ZAF1sUIrse-2C4rCifrCUOhGjzJN4Mr8pGZ1qRLMWVI/edit#gid=0

select 
       CURRENT_TIMESTAMP()
      ,DATETIME(CURRENT_TIMESTAMP(), "America/Los_Angeles") as adjusted

-- https://stackoverflow.com/questions/12482637/bigquery-converting-to-a-different-timezone