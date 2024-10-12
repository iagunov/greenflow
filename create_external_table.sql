CREATE EXTERNAL TABLE ext_counteragents_json (
    raw_json jsonb  -- JSONB для хранения всей строки JSON
)
LOCATION ('s3://your-bucket-name/path/to/jsonfile')
FORMAT 'TEXT' (DELIMITER ',')
LOG ERRORS SEGMENT REJECT LIMIT 1000 ROWS;

-- LOG ERRORS: записывает ошибки в лог
-- SEGMENT REJECT LIMIT: указывает, сколько строк с ошибками допускается при загрузке данных на каждом сегменте 
-- 1000 ROWS: лимит ошибок
