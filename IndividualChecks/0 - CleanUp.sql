IF OBJECT_ID('tempdb.dbo.#tmp1') IS NOT NULL
  DROP TABLE #tmp1

SELECT 'if object_id(''dbo.'+name+''') is not null drop table dbo.' + name AS SQLCmd
INTO #tmp1
FROM sys.tables WITH(NOLOCK)
WHERE name LIKE 'tmpStatisticCheck%'
OR name LIKE 'tmp_default_trace'

DECLARE @SQLCmd VARCHAR(800)
DECLARE c_cursor CURSOR STATIC FOR
    SELECT SQLCmd 
    FROM #tmp1
OPEN c_cursor

FETCH NEXT FROM c_cursor
INTO @SQLCmd
WHILE @@FETCH_STATUS = 0
BEGIN
  EXEC (@SQLCmd)

  FETCH NEXT FROM c_cursor
  INTO @SQLCmd
END
CLOSE c_cursor
DEALLOCATE c_cursor

