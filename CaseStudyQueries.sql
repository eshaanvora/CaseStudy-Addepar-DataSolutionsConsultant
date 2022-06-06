use AddeparCaseStudy;

DROP TABLE Transactions;
DROP TABLE TransactionTypeMappings;
DROP TABLE PositionDetails;
DROP TABLE SecurityTypeMappings;
DROP TABLE staging;

CREATE TABLE staging
SELECT Transactions.index, Security, temp.Addepar_Security_Type, 
TransactionTypeMappings.Addepar_Transaction_Type, Trade_Date, 
Quantity, Value, Currency
FROM Transactions
INNER JOIN TransactionTypeMappings
ON Transactions.Transaction_Type = TransactionTypeMappings.Transaction_Type
INNER JOIN (
SELECT PositionDetails.CUSIP, SecurityTypeMappings.Addepar_Security_Type
FROM PositionDetails
INNER JOIN SecurityTypeMappings
ON PositionDetails.Asset_Type = SecurityTypeMappings.Security_Type) temp
ON Transactions.CUSIP = temp.CUSIP;





