SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';


-- -----------------------------------------------------
-- Table `BookStore`.`ItemType`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`ItemType` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`ItemType` (
  `PKId` INT NOT NULL,
  `Name` VARCHAR(45) NULL DEFAULT NULL,
  `Description` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`PKId`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`Items`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`Items` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`Items` (
  `PKId` INT NOT NULL AUTO_INCREMENT,
  `Name` VARCHAR(45) NULL DEFAULT NULL,
  `TypedId` INT NOT NULL,
  `ImageFileSpec` VARCHAR(45) NULL DEFAULT NULL,
  `Description` VARCHAR(45) NULL DEFAULT NULL,
  `UnitCost` INT NULL DEFAULT 0,
  `UnitPrice` INT NULL DEFAULT 0,
  `ModifyDate` DATETIME NULL,
  PRIMARY KEY (`PKId`),
  INDEX `fk_Items_ItemType1_idx` (`TypedId` ASC),
  CONSTRAINT `fk_Items_ItemType1`
    FOREIGN KEY (`TypedId`)
    REFERENCES `BookStore`.`ItemType` (`PKId`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`Addresses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`Addresses` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`Addresses` (
  `PKid` INT NOT NULL AUTO_INCREMENT,
  `Address` VARCHAR(45) NULL DEFAULT NULL,
  `Country` VARCHAR(45) NULL DEFAULT NULL,
  `PhoneNumber` VARCHAR(45) NULL DEFAULT NULL,
  `Fax` VARCHAR(45) NULL DEFAULT NULL,
  `CustomerId` INT NULL,
  PRIMARY KEY (`PKid`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`Orders`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`Orders` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`Orders` (
  `PKId` INT NOT NULL AUTO_INCREMENT,
  `CustomerId` INT NULL,
  `Status` VARCHAR(45) NULL DEFAULT NULL,
  `OrderDate` DATETIME NULL,
  `ShippingHandling` VARCHAR(45) NULL DEFAULT NULL,
  `ShipToName` VARCHAR(45) NULL DEFAULT NULL,
  `ShipToAddressId` INT NOT NULL,
  `SubTotal` INT NULL DEFAULT 0,
  `Tax` INT NULL DEFAULT 0,
  `CreditCardType` VARCHAR(45) NULL DEFAULT NULL,
  `CreditCardNumber` VARCHAR(45) NULL DEFAULT NULL,
  `ExpirationDate` DATETIME NULL,
  `NameOnCard` VARCHAR(45) NULL DEFAULT NULL,
  `ApprovalCode` VARCHAR(45) NULL DEFAULT NULL,
  `ModifyDate` DATETIME NULL,
  PRIMARY KEY (`PKId`),
  INDEX `fk_Orders_Addresses1_idx` (`ShipToAddressId` ASC),
  CONSTRAINT `fk_Orders_Addresses1`
    FOREIGN KEY (`ShipToAddressId`)
    REFERENCES `BookStore`.`Addresses` (`PKid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`OrderItems`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`OrderItems` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`OrderItems` (
  `OrderID` INT NOT NULL,
  `ItemId` INT NOT NULL,
  `UnitPrice` INT NULL,
  `Quantity` INT NULL,
  `ONPRIMARY` VARCHAR(45) NULL,
  `ModifyDate` DATETIME NULL,
  INDEX `fk_OrderItems_Orders_idx` (`OrderID` ASC),
  INDEX `fk_OrderItems_Items1_idx` (`ItemId` ASC),
  CONSTRAINT `fk_OrderItems_Orders`
    FOREIGN KEY (`OrderID`)
    REFERENCES `BookStore`.`Orders` (`PKId`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_OrderItems_Items1`
    FOREIGN KEY (`ItemId`)
    REFERENCES `BookStore`.`Items` (`PKId`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`Categories`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`Categories` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`Categories` (
  `PKId` INT NOT NULL AUTO_INCREMENT,
  `ParentId` VARCHAR(45) NULL DEFAULT NULL,
  `Description` VARCHAR(45) NULL DEFAULT NULL,
  `IsLeaf` TINYINT(1) NULL,
  PRIMARY KEY (`PKId`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`ItemCategory`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`ItemCategory` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`ItemCategory` (
  `ModifyDate` DATETIME NULL,
  `ItemId` INT NOT NULL,
  `CateryId` INT NOT NULL,
  INDEX `fk_ItemCategory_Items1_idx` (`ItemId` ASC),
  INDEX `fk_ItemCategory_Categories1_idx` (`CateryId` ASC),
  CONSTRAINT `fk_ItemCategory_Items1`
    FOREIGN KEY (`ItemId`)
    REFERENCES `BookStore`.`Items` (`PKId`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_ItemCategory_Categories1`
    FOREIGN KEY (`CateryId`)
    REFERENCES `BookStore`.`Categories` (`PKId`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `BookStore`.`Clickstream_log`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `BookStore`.`Clickstream_log` ;

CREATE TABLE IF NOT EXISTS `BookStore`.`Clickstream_log` (
  `ipAddress` VARCHAR(45) NULL DEFAULT NULL,
  `uniqueId` INT NULL,
  `url` VARCHAR(45) NULL DEFAULT NULL,
  `SessionId` INT NOT NULL,
  `SessionTimes` INT NULL,
  `areaAddress` VARCHAR(45) NULL DEFAULT NULL,
  `localAddress` VARCHAR(45) NULL DEFAULT NULL,
  `browserType` VARCHAR(45) NULL DEFAULT NULL,
  `operationSys` VARCHAR(45) NULL DEFAULT NULL,
  `referUrl` VARCHAR(45) NULL DEFAULT NULL,
  `receiveTime` VARCHAR(45) NULL DEFAULT NULL,
  `userId` VARCHAR(45) NULL DEFAULT NULL,
  `csvp` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`SessionId`))
ENGINE = InnoDB;

--- add procdeure by titer1 ,and add random date (-5,0) (0,5) (90,95)

drop procedure  IF  EXISTS insert_Orders_For_alphaTest;

CREATE  PROCEDURE `insert_Orders_For_alphaTest`(IN num int)
begin
declare i int;
set i=0;
while i<num do
insert into Orders (PKId,CustomerId,ShipToAddressId,OrderDate,ExpirationDate,ModifyDate) values (rand()*1000,rand()*10,rand()*1000,curdate() - interval floor( rand()*5) day,curdate() + interval floor( rand()*5 + 90 ) day,curdate() + interval floor( rand()*5) day);
set i=i+1;
end while;
end$$

DELIMITER ;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

