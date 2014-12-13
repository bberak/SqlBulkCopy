USE [master]
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'Labs')
BEGIN
	ALTER DATABASE [Labs] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE [Labs]
END

CREATE DATABASE [Labs]

USE [Labs]
CREATE TABLE [dbo].[Person](
	[PersonId] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[DateOfBirth] [datetime] NULL,
	CONSTRAINT [PK_Person] PRIMARY KEY CLUSTERED ([PersonId] ASC)
)
