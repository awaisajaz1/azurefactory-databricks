CREATE TABLE [dbo].[landing_watermarks] (
	[id] INT NULL
	,[table_name] VARCHAR(200) NULL
	,[cdc_type] VARCHAR(50) NULL
	,[cdc_value] VARCHAR(200) NULL
	);

CREATE TABLE [dbo].[car_sales] (
	[Branch_ID] VARCHAR(200) NULL
	,[Dealer_ID] VARCHAR(200) NULL
	,[Model_ID] VARCHAR(200) NULL
	,[Revenue] BIGINT NULL
	,[Units_Sold] BIGINT NULL
	,[Date_ID] VARCHAR(200) NULL
	,[Day] INT NULL
	,[Month] INT NULL
	,[Year] INT NULL
	,[BranchName] VARCHAR(200) NULL
	,[DealerName] VARCHAR(300) NULL
	,[Product_Name] VARCHAR(300) NULL
	);
