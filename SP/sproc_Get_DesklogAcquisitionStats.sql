CREATE   PROCEDURE [dbo].[sproc_Get_DesklogAcquisitionStats]

	@lChildCompanyID INT,			--> Child Company Identifier

	@dtStart DATETIME,				--> Start Date Range

	@dtEnd DATETIME,				--> End Date Range

	@ActiveOverMinutes INT = 45		--> Config value for Timer - optional with default 45 minutes

AS







BEGIN



	SET NOCOUNT ON;		

    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	

	BEGIN TRY



		---==================================--

		--> Table Variables 

		---==================================--

		DECLARE @Companies TABLE (

			 lCompanyID INT

			,lChildCompanyID INT);

  		

		---==================================--      

		--> Temporary Tables

		---==================================--   

		DROP TABLE IF EXISTS #DesklogCore;

		CREATE TABLE #DesklogCore (

			 lDesklogVisitID BIGINT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,lDealID INT 

			,lTaskID BIGINT

			,Timer INT

			,dtIn DATETIME				-->[20231130 PM]

			,InTaskType SMALLINT );		-->[20231130 PM]



		DROP TABLE IF EXISTS #DesklogBeBack;

		CREATE TABLE #DesklogBeBack (			 

			 lDealID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,IsBeBack BIT);

		

		DROP TABLE IF EXISTS #Deal;

		CREATE TABLE #Deal (

			 lDealID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,lSourceID INT

			,dtBeBack SMALLDATETIME

			,IsSale BIT

			,IsAcquisition BIT

			,IsBought BIT);



		DROP TABLE IF EXISTS #Category;

		CREATE TABLE #Category (

			 lSourceID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,Showroom BIT

			,Campaign BIT

			,Internet BIT

			,Phone BIT);



		DROP TABLE IF EXISTS #Sold;

		CREATE TABLE #Sold (

			 lDealID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,dtSold SMALLDATETIME

			,InRange BIT 

			,curFrontGross MONEY

			,curTotalGross MONEY);



		DROP TABLE IF EXISTS #SalesProcess;

		CREATE TABLE #SalesProcess(			 	

			 lDealID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)			

			,WriteUp BIT

			,Demo BIT

			--,InShowroom BIT		-->[20231130 PM]

			,Appraisal BIT

			,TurnOver BIT);

		

		-->[20231130 PM]

		DROP TABLE IF EXISTS #SalesInShowroom;  

		CREATE TABLE #SalesInShowroom(       		 		 

			 lDealID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,InShowroom SMALLINT);



		DROP TABLE IF EXISTS #DealAdditionalStats;

		CREATE TABLE #DealAdditionalStats (

			 lDealID INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)

			,TimerOver BIT);

					

		---==================================--      

		-- Find All Companies to Display

		---==================================-- 		     		

		INSERT INTO @Companies (

			 lCompanyID 

			,lChildCompanyID ) 

		SELECT          

			lCompanyID = COALESCE(ccm.lCompanyID, c.lCompanyID)          

			,lChildCompanyID = COALESCE(ccm.lChildCompanyID, c.lCompanyID)          			         

		FROM dbo.tblCompany c

			INNER JOIN dbo.tblCompanyDetails cd

				ON cd.lCompanyID = c.lCompanyID

			LEFT JOIN dbo.tblCompanyChildCompanyMap ccm

				ON ccm.lChildCompanyID = c.lCompanyID

		WHERE EXISTS (SELECT 1 FROM dbo.vwCompanyHierarchy ch

						WHERE c.lCompanyID = ch.lChildID

						AND ch.lParentID = @lChildCompanyID)

		AND c.bActive = 1;

			

		---==================================--      

		-- Get Desklog Visits (CORE)

		-->New logic added for dtIn and InTaskType Calc [20231130 PM]		

		---==================================-- 

		INSERT INTO #DesklogCore (

			 lDesklogVisitID

			 ,lDealID

			 ,lTaskID

			 ,Timer

			 ,dtIn					

			 ,InTaskType)			

		SELECT v.lDesklogVisitID

			,v.lDealID

			,v.lTaskID

			,Timer = CASE WHEN t.lTaskTypeID IN (7 --> Appointment  

											,8 --> Lot Up / Showroom Up  

											,31 ) --> Showroom Visit  

							AND t.dtCompleted IS NOT NULL AND v.dtOut IS NULL --> Only those not marked out will be considered  

								THEN DATEDIFF (MINUTE, v.dtIn, SYSDATETIME()) END --> When clock is still ticking  			

			,v.dtIn	

			,InTaskType = CASE WHEN t.lTaskTypeID IN (7 --> Appointment  

											,8 --> Lot Up / Showroom Up  

											,31 ) --> Showroom Visit 

								THEN 1 ELSE NULL END		

		FROM dbo.tblDesklogVisit v 

			LEFT JOIN dbo.vwTask t

				ON t.lTaskID = v.lTaskID		 

		WHERE EXISTS (SELECT 1 FROM @Companies c 

						WHERE c.lChildCompanyID = v.lCompanyID

						AND c.lCompanyID = t.lCompanyID)

		--> [20240125 PM (2)]

		AND EXISTS ( SELECT 1 FROM dbo.vwDeal d

				WHERE d.lDealID = v.lDealID

				AND t.lCustomerID = d.lPersonID) 

		AND v.dtIn BETWEEN @dtStart AND @dtEnd

		AND v.lScratch = 0;		



		

		---==================================--      

		-- Get Desklog Be Back 

		--> New Logic added to calculate the BeBack Logic [20231130 PM]

		---==================================-- 

		INSERT INTO #DesklogBeBack(			

			 lDealID

			,IsBeBack)

		SELECT lDealID  

			,IsBeBack = COUNT(1)  --> [20240125 PM (3)]

		FROM #DesklogCore dc  

		WHERE InTaskType IS NOT NULL  --> [20240125 PM (1, 4)]

		AND EXISTS (SELECT 1 FROM dbo.vwTask t 										

					INNER JOIN dbo.vwDeal d 

						ON d.lDealID = t.lDealID					

					INNER JOIN dbo.tblDesklogVisit v2 

						ON v2.lTaskID = t.lTaskID

						AND v2.dtIN < dc.dtIN

					WHERE t.lDealID = dc.lDealID 

					--> Only Appt, Showroom, and Visits are considered   

						AND t.lTaskTypeID IN (7 --> Appointment  

							,8 --> Lot Up / Showroom Up  

							,31 ) --> Showroom Visit  

						AND t.dtCompleted < dc.dtIn)

		GROUP BY lDealID;

				 		

		---==================================--      

		-- Calculate Deals

		---==================================-- 

		INSERT INTO #Deal (

			 lDealID			

			,lSourceID

			,dtBeBack

			,IsSale

			,IsAcquisition

			,IsBought) 

		SELECT d.lDealID

			,d.lSourceID

			,d.dtBeBack

			,IsSale = CASE WHEN d.nliColorID != 4584 THEN 1 ELSE 0 END --> [4584 - Acquisition]

			,IsAcquisition = CASE WHEN d.nliColorID = 4584 THEN 1 ELSE 0 END --> [4584 - Acquisition]

			,IsBought = CASE WHEN d.nliColorID = 4584 --> [4584 - Acquisition]

								AND ss.lDealSubStatusID IS NOT NULL  --> It's status needs to be Bought and Active

								AND dd.dtSubStatusChange BETWEEN @dtStart AND @dtEnd THEN 1 ELSE 0 END --> Needs to be in Range so the Bought counts

		FROM dbo.vwDeal d

			INNER JOIN dbo.vwDealDetails dd 

				ON d.lDealID = dd.lDealID

			LEFT JOIN dbo.tblDealSubStatus ss

				ON ss.szDealSubStatus = 'Bought'  --> Any other SubStatus should not be considered an Acquisition

				AND ss.bActive = 1

				AND ss.nliColorID = 4584 --> [4584 - Acquisition]

				AND ss.lDealSubStatusID = dd.lDealSubStatusID

		WHERE EXISTS (SELECT 1 FROM #DesklogCore dc 

						WHERE dc.lDealID = d.lDealID)

		AND EXISTS (SELECT 1 FROM dbo.tblPerson p 

						WHERE d.lPersonID = p.lPersonID

						AND p.bActive = 1);

					

		---==================================--

		-- Calculate Category

		---==================================--

		INSERT INTO #Category (

			 lSourceID

			,Showroom

			,Campaign

			,Internet

			,Phone)		 

		SELECT lSourceID 

			,Showroom = [20]

			,Campaign = [21]

			,Internet = [22]

			,Phone = [23]			

		FROM (

			SELECT s.lSourceID

				,Marked = 1

				,s.nliCategoryID

			FROM dbo.tblSource s			

			WHERE EXISTS (SELECT 1 FROM #Deal d WHERE s.lSourceID = d.lSourceID)			

			) src

		PIVOT (MAX (Marked) FOR nliCategoryID IN ([20] --> Showroom Up 

												, [21] --> Campaign

												, [22] --> Internet Up

												, [23])--> Phone Up

		) as p;



		---==================================--

		-- Calculate Sold

		---==================================--

		INSERT INTO #Sold(

			 lDealID

			,dtSold

			,InRange

			,curFrontGross

			,curTotalGross)

		SELECT p.lDealID

			,p.dtSold

			,InRange = CASE WHEN p.dtSold BETWEEN @dtStart AND @dtEnd THEN 1 ELSE 0 END

			,p.curFrontGross

			,p.curTotalGross

		FROM dbo.tblPurchaseDetails p

		WHERE EXISTS (SELECT 1 FROM #Deal d WHERE p.lDealID = d.lDealID);



		---==================================--

		-- Calculate Write Up, Demo, & Appraisal 

		--> In showroom removed [20231130 PM]

		---==================================--

		INSERT INTO #SalesProcess (			 

			 lDealID			

			,WriteUp

			,Demo

			--,InShowroom				--> [20231130 PM]

			,Appraisal

			,TurnOver) 

		SELECT lDealID						

			,WriteUp = [163]

			,Demo = [164]

			--,InShowroom = [254]		--> [20231130 PM]

			,Appraisal = [280]

			,TurnOver = [162]			

		FROM (

				SELECT i.lDealID

					,Marked = 1						

					,i.nliListItemID					

				FROM dbo.vwTaskItem i

				WHERE EXISTS (SELECT 1 FROM #DesklogCore dc	

								WHERE dc.lDealID = i.lDealID

								AND dc.InTaskType = 1)  --> [20231130 PM]				

			) src

		PIVOT (MAX (Marked) FOR nliListItemID IN ([163] --> Write Up

												, [164]	--> Demo

												--, [254]	--> In Showroom

												, [280] --> Appraisal 

												, [162])--> Turn Over 

		) as p;

			

			

		---==================================--  

		-- Calculate In Showroom

		-->In showroom calc added [20231130 PM]  

		---==================================--  

		INSERT INTO #SalesInShowroom (		 

			 lDealID

			,InShowroom)

		SELECT lDealID

			,InShowroom = SUM(InTaskType)

		FROM #DesklogCore dc

		WHERE dc.InTaskType = 1

		GROUP BY lDealID;		

	

		---==================================--

		-- Calculate Additional Stats

		---==================================--

		INSERT INTO #DealAdditionalStats (

			 lDealID

			,TimerOver)

		SELECT lDealID

			,TimerOver = MAX(CASE WHEN (dc.Timer) > @ActiveOverMinutes THEN 1 ELSE NULL END) --> Over mins value from desklog config --> 20220928 MLM

		FROM #DesklogCore dc		

		GROUP BY lDealID;



						

		---==================================--

		-- Stats Result

		--BASED ON DEALS 

		---==================================--				

		SELECT  Sold_Sales = COUNT (CASE WHEN d.IsSale = 1 AND s.InRange = 1 THEN d.lDealID ELSE NULL END) 

			,Sold_Showroom_Sales = COUNT (CASE WHEN d.IsSale = 1 AND c.Showroom = 1 AND s.InRange = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Internet_Sales = COUNT (CASE WHEN d.IsSale = 1 AND c.Internet = 1 AND s.InRange = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Phone_Sales = COUNT (CASE WHEN d.IsSale = 1 AND c.Phone = 1 AND s.InRange = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Campaign_Sales = COUNT (CASE WHEN d.IsSale = 1 AND c.Campaign = 1 AND s.InRange = 1 THEN d.lDealID ELSE NULL END)			

						

			,BeBack_Sales = COUNT(CASE WHEN d.IsSale = 1 AND bb.IsBeBack = 1 THEN 1 ELSE NULL END)		

			,Showroom_Sales = SUM (CASE WHEN d.IsSale = 1 THEN ss.InShowroom ELSE NULL END)

			,Campaign_Sales = COUNT (CASE WHEN d.IsSale = 1 AND ss.InShowroom IS NULL THEN c.Campaign ELSE NULL END)

			,Phone_Sales = COUNT (CASE WHEN d.IsSale = 1 AND ss.InShowroom IS NULL THEN c.Phone ELSE NULL END)

			,Internet_Sales = COUNT (CASE WHEN d.IsSale = 1 AND ss.InShowroom IS NULL THEN c.Internet ELSE NULL END)



			,WriteUp_Sales = COUNT (CASE WHEN d.IsSale = 1 THEN sp.WriteUp ELSE NULL END)

			,Demo_Sales = COUNT (CASE WHEN d.IsSale = 1 THEN sp.Demo ELSE NULL END)			

			,Appraisal_Sales = COUNT (CASE WHEN d.IsSale = 1 THEN sp.Appraisal ELSE NULL END)

			,Over45Min_Sales = COUNT(CASE WHEN  d.IsSale = 1 THEN a.TimerOver ELSE NULL END)

			,TurnOver_Sales = COUNT(CASE WHEN d.IsSale = 1 THEN sp.TurnOver ELSE NULL END)



			--> The Notes below will be reviewed by Business, but since this stored proc is meant to replace the current logic we cannot change the business logic until further notice.

			,FrontGross_Sales = SUM(CASE WHEN d.IsSale = 1 AND s.dtSold IS NOT NULL THEN s.curFrontGross ELSE 0.00 END) --** This does not take in consideration any range for sales other than the Desklog Visit **CURRENT VALUE (Wrong)**

			,TotalGross_Sales = SUM(CASE WHEN d.IsSale = 1 AND s.dtSold IS NOT NULL THEN s.curTotalGross ELSE 0.00 END) --** This does not take in consideration any range for sales other than the Desklog Visit **CURRENT VALUE (Wrong)**		

			--> This logic will replace the code avobe once business agrees on the change

			--,FrontGross_Sales = SUM(CASE WHEN d.IsSale = 1 AND s.InRange = 1 THEN s.curFrontGross ELSE 0.00 END) --** This uses Sold Date to validate Range

			--,TotalGross_Sales = SUM(CASE WHEN d.IsSale = 1 AND s.InRange = 1 THEN s.curTotalGross ELSE 0.00 END) --** This uses Sold Date to validate Range



			,Sold_Acq = COUNT (CASE WHEN d.IsBought = 1 AND d.IsAcquisition = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Showroom_Acq = COUNT (CASE WHEN d.IsBought = 1 AND d.IsAcquisition = 1 AND c.Showroom = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Internet_Acq = COUNT (CASE WHEN d.IsBought = 1 AND d.IsAcquisition = 1 AND c.Internet = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Phone_Acq = COUNT (CASE WHEN d.IsBought = 1 AND d.IsAcquisition = 1 AND c.Phone = 1 THEN d.lDealID ELSE NULL END)

			,Sold_Campaign_Acq = COUNT (CASE WHEN d.IsBought = 1 AND d.IsAcquisition = 1 AND c.Campaign = 1 THEN d.lDealID ELSE NULL END)			

			

			,BeBack_Acq = COUNT(CASE WHEN d.IsAcquisition = 1 AND bb.IsBeBack = 1 THEN d.lDealID ELSE NULL END)		

			,Showroom_Acq = SUM (CASE WHEN d.IsAcquisition = 1 THEN ss.InShowroom ELSE NULL END)

			,Campaign_Acq = COUNT (CASE WHEN d.IsAcquisition = 1 AND ss.InShowroom IS NULL THEN c.Campaign ELSE NULL END)

			,Phone_Acq = COUNT (CASE WHEN d.IsAcquisition = 1 AND ss.InShowroom IS NULL THEN c.Phone ELSE NULL END)

			,Internet_Acq = COUNT (CASE WHEN d.IsAcquisition = 1 AND ss.InShowroom IS NULL THEN c.Internet ELSE NULL END)



			,WriteUp_Acq = COUNT (CASE WHEN d.IsAcquisition = 1 THEN sp.WriteUp ELSE NULL END)

			,Demo_Acq = COUNT (CASE WHEN d.IsAcquisition = 1 THEN sp.Demo ELSE NULL END)			

			,Appraisal_Acq = COUNT (CASE WHEN d.IsAcquisition = 1 THEN sp.Appraisal ELSE NULL END)

			,Over45Min_Acq = COUNT(CASE WHEN  d.IsAcquisition = 1 THEN a.TimerOver ELSE NULL END)

			,TurnOver_Acq = COUNT(CASE WHEN d.IsAcquisition = 1 THEN sp.TurnOver ELSE NULL END)

												

		FROM #Deal d 						

			LEFT JOIN #Category c

				ON c.lSourceID = d.lSourceID

			LEFT JOIN #Sold s

				ON s.lDealID = d.lDealID

			LEFT JOIN #SalesProcess sp

				ON sp.lDealID = d.lDealID

			LEFT JOIN #SalesInShowroom ss

				ON ss.lDealID = d.lDealID

			LEFT JOIN #DealAdditionalStats a

				ON a.lDealID = d.lDealID

			LEFT JOIN #DesklogBeBack bb

				ON bb.lDealID = d.lDealID;

		

	END TRY

	BEGIN CATCH 

		

		THROW;



	END CATCH; 



END;