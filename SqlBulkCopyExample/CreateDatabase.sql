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
	[AreaCode] [nvarchar](100) NOT NULL,
	[Number] [nvarchar](100) NOT NULL,
	[FullPhoneNumber] [nvarchar](200) NOT NULL,
	[HasKids] [bit] NOT NULL DEFAULT 0,
	[SumOfKidsAge] [int] NOT NULL DEFAULT 0,
	CONSTRAINT [PK_Person] PRIMARY KEY CLUSTERED ([PersonId] ASC)
)

CREATE TABLE [dbo].[Kid](
	[KidId] [int] IDENTITY(1, 1) NOT NULL PRIMARY KEY,
	[PersonId] [int] NOT NULL,
	[Age] [int] NOT NULL,
	CONSTRAINT [FK_Kid_To_Person] FOREIGN KEY ([PersonId]) REFERENCES [Person]([PersonId]),

)
