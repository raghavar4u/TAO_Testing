pseudo
PROCEDURE sproc_Get_DesklogAcquisitionStats(lChildCompanyID, dtStart, dtEnd, ActiveOverMinutes = 45)
BEGIN
    -- Setup
    SET NOCOUNT ON
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

    TRY
        -- Declare table variable for companies
        DECLARE TABLE @Companies

        -- Create temporary tables to store intermediate data
        DROP IF EXISTS #DesklogCore
        CREATE TABLE #DesklogCore
        ...

        DROP IF EXISTS #DesklogBeBack
        CREATE TABLE #DesklogBeBack
        ...

        DROP IF EXISTS #Deal
        CREATE TABLE #Deal
        ...

        DROP IF EXISTS #Category
        CREATE TABLE #Category
        ...

        DROP IF EXISTS #Sold
        CREATE TABLE #Sold
        ...

        DROP IF EXISTS #SalesProcess
        CREATE TABLE #SalesProcess
        ...

        DROP IF EXISTS #SalesInShowroom
        CREATE TABLE #SalesInShowroom
        ...

        DROP IF EXISTS #DealAdditionalStats
        CREATE TABLE #DealAdditionalStats
        ...
        
        -- Identify Companies
        INSERT INTO @Companies
        SELECT FROM tblCompany, tblCompanyDetails, tblCompanyChildCompanyMap
        WHERE EXISTS (Company Hierarchy Matches) AND (Company is Active)

        -- Populate Desklog Visits Core data
        INSERT INTO #DesklogCore
        SELECT FROM tblDesklogVisit, vwTask
        WHERE (Criteria based on Company match, TaskType and Date Range)

        -- Calculate Desklog Be Back Logic
        INSERT INTO #DesklogBeBack
        SELECT FROM #DesklogCore, vwTask, vwDeal
        WHERE (InTaskType is not null and previous visit criteria matches)

        -- Calculate Deals related data
        INSERT INTO #Deal
        SELECT FROM vwDeal, vwDealDetails, tblDealSubStatus
        WHERE (Deal and Status criteria matches)

        -- Calculate Source Category
        INSERT INTO #Category
        SELECT FROM tblSource
        PIVOT (Max based on Category ID)

        -- Calculate Deals Sold data
        INSERT INTO #Sold
        SELECT FROM tblPurchaseDetails
        WHERE (Exists in Deal Table)

        -- Calculate Sales Process Stages (Write Up, Demo, etc.)
        INSERT INTO #SalesProcess
        SELECT FROM vwTaskItem
        PIVOT (Max based on ListItemID)

        -- Calculate In Showroom Visits
        INSERT INTO #SalesInShowroom
        SELECT FROM #DesklogCore
        WHERE (InTaskType = 1)

        -- Calculate Additional Deal Stats (Timer over threshold)
        INSERT INTO #DealAdditionalStats
        SELECT FROM #DesklogCore
        WHERE (Timer greater than ActiveOverMinutes)

        -- Compile final statistics result set
        SELECT Calculate all required sales and acquisition stats
        FROM #Deal, #Category, #Sold, #SalesProcess, #SalesInShowroom, #DealAdditionalStats, #DesklogBeBack
        LEFT JOIN other temporary tables as required

    CATCH
        -- Handle any errors
        THROW EXCEPTION

    END TRY-CATCH

END PROCEDURE;