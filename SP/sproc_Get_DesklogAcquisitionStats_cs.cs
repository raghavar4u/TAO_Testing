csharp
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Linq;

// Input Request Parameters
public class DesklogAcquisitionStatsRequest
{
    public int ChildCompanyID { get; set; } // Child Company Identifier
    public DateTime StartDate { get; set; } // Start Date Range
    public DateTime EndDate { get; set; }   // End Date Range
    public int ActiveOverMinutes { get; set; } = 45; // Config value for Timer - optional with default 45 minutes
}

// Output Response Parameters
public class DesklogAcquisitionStatsResponse
{
    public int Sold_Sales { get; set; }
    public int Sold_Showroom_Sales { get; set; }
    public int Sold_Internet_Sales { get; set; }
    public int Sold_Phone_Sales { get; set; }
    public int Sold_Campaign_Sales { get; set; }
    public int BeBack_Sales { get; set; }
    public int Showroom_Sales { get; set; }
    public int Campaign_Sales { get; set; }
    public int Phone_Sales { get; set; }
    public int Internet_Sales { get; set; }
    public int WriteUp_Sales { get; set; }
    public int Demo_Sales { get; set; }
    public int Appraisal_Sales { get; set; }
    public int Over45Min_Sales { get; set; }
    public int TurnOver_Sales { get; set; }
    public decimal FrontGross_Sales { get; set; }
    public decimal TotalGross_Sales { get; set; }
    public int Sold_Acq { get; set; }
    public int Sold_Showroom_Acq { get; set; }
    public int Sold_Internet_Acq { get; set; }
    public int Sold_Phone_Acq { get; set; }
    public int Sold_Campaign_Acq { get; set; }
    public int BeBack_Acq { get; set; }
    public int Showroom_Acq { get; set; }
    public int Campaign_Acq { get; set; }
    public int Phone_Acq { get; set; }
    public int Internet_Acq { get; set; }
    public int WriteUp_Acq { get; set; }
    public int Demo_Acq { get; set; }
    public int Appraisal_Acq { get; set; }
    public int Over45Min_Acq { get; set; }
    public int TurnOver_Acq { get; set; }
}

// Context that matches your database schema
public class DataContext : DbContext
{
    public DbSet<Company> Companies { get; set; }
    public DbSet<CompanyDetail> CompanyDetails { get; set; }
    public DbSet<CompanyChildCompanyMap> CompanyChildCompanyMaps { get; set; }
    public DbSet<vwCompanyHierarchy> CompanyHierarchies { get; set; }
    public DbSet<DesklogVisit> DesklogVisits { get; set; }
    public DbSet<vwTask> Tasks { get; set; }
    public DbSet<vwDeal> Deals { get; set; }
    public DbSet<vwDealDetail> DealDetails { get; set; }
    public DbSet<tblDealSubStatus> DealSubStatuses { get; set; }
    public DbSet<tblPurchaseDetail> PurchaseDetails { get; set; }
    public DbSet<vwTaskItem> TaskItems { get; set; }
    public DbSet<tblSource> Sources { get; set; }
    public DbSet<tblPerson> Persons { get; set; }
    public DbSet<DealAdditionalStat> DealAdditionalStats { get; set; }

    // Define other DbSets as needed...
}

// Define models consistent with the database schema
public class Company { public int lCompanyID { get; set; } public int lChildCompanyID { get; set; } public bool bActive { get; set; } }
public class CompanyDetail { public int lCompanyID { get; set; } }
public class CompanyChildCompanyMap { public int lChildCompanyID { get; set; } public int lCompanyID { get; set; } }
public class vwCompanyHierarchy { public int lChildID { get; set; } public int lParentID { get; set; } }
public class DesklogVisit { public long lDesklogVisitID { get; set; } public int lDealID { get; set; } public long lTaskID { get; set; } public DateTime dtIn { get; set; } public DateTime? dtOut { get; set; } public int lCompanyID { get; set; } public int lScratch { get; set; } }
public class vwTask { public long lTaskID { get; set; } public int lTaskTypeID { get; set; } public DateTime? dtCompleted { get; set; } public int lCompanyID { get; set; } public int lCustomerID { get; set; } public int lDealID { get; set; } }
public class vwDeal { public int lDealID { get; set; } public DateTime dtBeBack { get; set; } public int lPersonID { get; set; } public int lSourceID { get; set; } public int nliColorID { get; set; } }
public class tblDealSubStatus { public string szDealSubStatus { get; set; } public bool bActive { get; set; } public int nliColorID { get; set; } public int lDealSubStatusID { get; set; } }
public class vwDealDetail { public int lDealID { get; set; } public DateTime? dtSubStatusChange { get; set; } public int lDealSubStatusID { get; set; } }
public class tblPurchaseDetail { public int lDealID { get; set; } public DateTime dtSold { get; set; } public decimal curFrontGross { get; set; } public decimal curTotalGross { get; set; } }
public class vwTaskItem { public int lDealID { get; set; } public int nliListItemID { get; set; } }
public class tblSource { public int lSourceID { get; set; } public int nliCategoryID { get; set; } }
public class tblPerson { public int lPersonID { get; set; } public bool bActive { get; set; } }

public class LinqConversions
{
    public static DesklogAcquisitionStatsResponse GetDesklogAcquisitionStats(DataContext context, DesklogAcquisitionStatsRequest request)
    {
        // Find All Companies to Display
        var companyQuery = from c in context.Companies
                           join cd in context.CompanyDetails on c.lCompanyID equals cd.lCompanyID
                           join ccm in context.CompanyChildCompanyMaps on c.lCompanyID equals ccm.lChildCompanyID into companyMap
                           from cm in companyMap.DefaultIfEmpty()
                           where context.CompanyHierarchies.Any(ch => c.lCompanyID == ch.lChildID && ch.lParentID == request.ChildCompanyID)
                                 && c.bActive
                           select new { lCompanyID = cm?.lCompanyID ?? c.lCompanyID, lChildCompanyID = cm?.lChildCompanyID ?? c.lCompanyID };

        List<int> companyIds = companyQuery.Select(x => x.lChildCompanyID).Distinct().ToList();

        // Get Desklog Core
        var desklogCoreQuery = from visit in context.DesklogVisits
                               join task in context.Tasks on visit.lTaskID equals task.lTaskID into tasks
                               from t in tasks.DefaultIfEmpty()
                               where companyIds.Contains(visit.lCompanyID)
                                     && t != null
                                     && Object.Equals(t.lCompanyID, visit.lCompanyID)
                                     && t.lCustomerID == (context.Deals.Where(d => d.lDealID == visit.lDealID).Select(d => d.lPersonID).FirstOrDefault())
                                     && visit.dtIn >= request.StartDate && visit.dtIn <= request.EndDate
                                     && visit.lScratch == 0
                               select new
                               {
                                   visit.lDesklogVisitID,
                                   visit.lDealID,
                                   visit.lTaskID,
                                   Timer = t.lTaskTypeID != null &&
                                   t.lTaskTypeID is var type && (type == 7 || type == 8 || type == 31) &&
                                   t.dtCompleted != null && visit.dtOut == null ?
                                   DbFunctions.DiffMinutes(visit.dtIn, DateTime.Now) : default(int?),
                                   visit.dtIn,
                                   InTaskType = t.lTaskTypeID != null &&
                                   (t.lTaskTypeID == 7 || t.lTaskTypeID == 8 || t.lTaskTypeID == 31) ? 1 : default(int?)
                               };

        // Get Desklog Be Back
        var desklogBeBackQuery = from dc in desklogCoreQuery
                                 where dc.InTaskType != null
                                 where context.Tasks.Any(t => context.Deals.Any(d => d.lDealID == t.lDealID) &&
                                   context.DesklogVisits.Any(v => v.lTaskID == t.lTaskID && v.dtIn < dc.dtIn) &&
                                   t.lDealID == dc.lDealID &&
                                   (t.lTaskTypeID == 7 || t.lTaskTypeID == 8 || t.lTaskTypeID == 31) &&
                                   t.dtCompleted < dc.dtIn)
                                 group dc by dc.lDealID
                                 into g
                                 select new
                                 {
                                     g.Key,
                                     IsBeBack = g.Count()
                                 };

        // Calculate Deals
        var dealQuery = from deal in context.Deals
                        join dd in context.DealDetails on deal.lDealID equals dd.lDealID
                        join ss in context.DealSubStatuses on dd.lDealSubStatusID equals ss.lDealSubStatusID into statuses
                        from status in statuses.DefaultIfEmpty()
                        where desklogCoreQuery.Any(dc => dc.lDealID == deal.lDealID)
                              && context.Persons.Any(p => p.lPersonID == deal.lPersonID && p.bActive)
                        select new
                        {
                            deal.lDealID,
                            deal.lSourceID,
                            deal.dtBeBack,
                            IsSale = deal.nliColorID != 4584,
                            IsAcquisition = deal.nliColorID == 4584,
                            IsBought = deal.nliColorID == 4584 && status.bActive && status.nliColorID == 4584 && status.szDealSubStatus == "Bought" && status.lDealSubStatusID == dd.lDealSubStatusID && dd.dtSubStatusChange >= request.StartDate && dd.dtSubStatusChange <= request.EndDate
                        };

        // Calculate Category
        var categoryQuery = from source in context.Sources
                            where dealQuery.Any(d => d.lSourceID == source.lSourceID)
                            group source by source.lSourceID
                            into g
                            select new
                            {
                                lSourceID = g.Key,
                                Showroom = g.Any(x => x.nliCategoryID == 20),
                                Campaign = g.Any(x => x.nliCategoryID == 21),
                                Internet = g.Any(x => x.nliCategoryID == 22),
                                Phone = g.Any(x => x.nliCategoryID == 23)
                            };

        // Calculate Sold
        var soldQuery = from purchase in context.PurchaseDetails
                        where dealQuery.Any(d => d.lDealID == purchase.lDealID)
                        select new
                        {
                            purchase.lDealID,
                            purchase.dtSold,
                            InRange = (purchase.dtSold >= request.StartDate && purchase.dtSold <= request.EndDate),
                            purchase.curFrontGross,
                            purchase.curTotalGross
                        };

        // Calculate Sales Process
        var salesProcessQuery = from item in context.TaskItems
                                 where desklogCoreQuery.Any(dc => dc.lDealID == item.lDealID && dc.InTaskType == 1)
                                 group item by item.lDealID
                                 into g
                                 select new
                                 {
                                     lDealID = g.Key,
                                     WriteUp = g.Any(x => x.nliListItemID == 163) ? (bool?)true : null,
                                     Demo = g.Any(x => x.nliListItemID == 164) ? (bool?)true : null,
                                     Appraisal = g.Any(x => x.nliListItemID == 280) ? (bool?)true : null,
                                     TurnOver = g.Any(x => x.nliListItemID == 162) ? (bool?)true : null
                                 };

        // Calculate In Showroom
        var salesInShowroomQuery = from dc in desklogCoreQuery
                                   where dc.InTaskType == 1
                                   group dc by dc.lDealID
                                   into g
                                   select new
                                   {
                                       g.Key,
                                       InShowroom = g.Count()
                                   };

        // Calculate Additional Stats
        var dealAdditionalStatsQuery = from dc in desklogCoreQuery
                                       group dc by dc.lDealID
                                       into g
                                       select new
                                       {
                                           g.Key,
                                           TimerOver = g.Max(x => x.Timer) > request.ActiveOverMinutes ? (bool?)true : null
                                       };

        // Final Select/Join for output
        var finalQuery = from d in dealQuery
                         join c in categoryQuery on d.lSourceID equals c.lSourceID into categories
                         from cat in categories.DefaultIfEmpty()
                         join s in soldQuery on d.lDealID equals s.lDealID into solds
                         from sale in solds.DefaultIfEmpty()
                         join sp in salesProcessQuery on d.lDealID equals sp.lDealID into salesProcesses
                         from salesProcess in salesProcesses.DefaultIfEmpty()
                         join ss in salesInShowroomQuery on d.lDealID equals ss.Key into showrooms
                         from showroom in showrooms.DefaultIfEmpty()
                         join a in dealAdditionalStatsQuery on d.lDealID equals a.Key into additionalStats
                         from additional in additionalStats.DefaultIfEmpty()
                         join bb in desklogBeBackQuery on d.lDealID equals bb.Key into beBacks
                         from beBack in beBacks.DefaultIfEmpty()
                         select new
                         {
                             // Fill in the properties as needed
                             Sold_Sales = (d.IsSale && sale != null && sale.InRange) ? 1 : 0,
                             // Implement other calculations like these and return final list
                         };

        DesklogAcquisitionStatsResponse response = new DesklogAcquisitionStatsResponse();

        // Aggregate the results from finalQuery and map them to DesklogAcquisitionStatsResponse
        foreach (var result in finalQuery)
        {
            response.Sold_Sales += result.Sold_Sales;
            // Continue assigning other calculated fields' results here...
        }

        return response;
    }
}