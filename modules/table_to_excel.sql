DECLARE @tableName NVARCHAR(100)
DECLARE tableCursor CURSOR FOR
    SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'

OPEN tableCursor

FETCH NEXT FROM tableCursor INTO @tableName

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @sql NVARCHAR(MAX)
    SET @sql = 'SELECT * INTO [YourDestinationFolder\' + @tableName + '.xlsx] FROM ' + @tableName
    EXEC(@sql)

    FETCH NEXT FROM tableCursor INTO @tableName
END

CLOSE tableCursor
DEALLOCATE tableCursor
