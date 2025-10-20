# **Naming Conventions**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## **Table of Contents**

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Staging Rules](#staging-rules)
   - [Transformation Rules](#transformation-rules)
   - [reporting Rules](#reporting-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedure](#stored-procedure-naming-conventions)
---

## **General Principles**

- **Naming Conventions**: Use snake_case, with lowercase letters and underscores (`_`) to separate words.
- **Language**: Use English for all names.
- **Avoid Reserved Words**: Do not use SQL reserved words as object names.

## **Table Naming Conventions**

### **Staging Rules**
- All names must start with the source system name, and table names must match their original names without renaming.
- **`<sourcesystem>_<entity>`**  
  - `<sourcesystem>`: Name of the source system (e.g., `nba`, `nfl`).  
  - `<entity>`: Exact table name from the source system.  
  - Example: `nba_players` → Player information from the NBA data source.

### **Transformation Rules**
- All names must start with the source system name, and table names must match their original names without renaming.
- **`<sourcesystem>_<entity>`**  
  - `<sourcesystem>`: Name of the source system (e.g., `nba`, `nfl`).  
  - `<entity>`: Exact table name from the source system.  
  - Example: `nba_players` → Player information from the NBA data source.

### **Reporting Rules**
- All names must use meaningful, purpose-aligned names for tables, starting with the category prefix.
- **`<category>_<entity>`**  
  - `<category>`: Describes the role of the table, such as `dim` (dimension) or `fact` (fact table).  
  - `<entity>`: Descriptive name of the table, aligned with the business domain (e.g., `players`, `team_details`, `team_gamelog`).  
  - Examples:
    - `dim_team_gamelogs` → Dimension table for team data. 
    - `dim_player_gamelogs` → Dimension table for player data.   
    - `fact_players` → Fact table containing player information. 
    - `fact_teams` → Fact table containing team information. 

#### **Glossary of Category Patterns**

| Pattern     | Meaning                           | Example(s)                              |
|-------------|-----------------------------------|-----------------------------------------|
| `dim_`      | Dimension table                  | `dim_team_gamelogs`, `dim_player_gamelogs`           |
| `fact_`     | Fact table                       | `fact_teams`                            |
| `report_`   | Report table                     | `report_top_player_performances`  |

## **Column Naming Conventions**

### **Surrogate Keys**  
- All primary keys in dimension tables must use the suffix `_key`.
- **`<table_name>_key`**  
  - `<table_name>`: Refers to the name of the table or entity the key belongs to.  
  - `_key`: A suffix indicating that this column is a surrogate key.  
  - Example: `team_id_key` → Surrogate key in the `dim_team_gamelogs` table.
  
### **Technical Columns**
- All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column's purpose.
- **`dwh_<column_name>`**  
  - `dwh`: Prefix exclusively for system-generated metadata.  
  - `<column_name>`: Descriptive name indicating the column's purpose.  
  - Example: `dwh_load_date` → System-generated column used to store the date when the record was loaded.
 
## **Stored Procedure**

- All stored procedures used for loading data must follow the naming pattern:
- **`load_<layer>`**.
  
  - `<layer>`: Represents the layer being loaded, such as `staging`, `transformation`, or `reporting`.
  - Example: 
    - `load_staging` → Stored procedure for loading data into the Staging layer.
    - `load_transformation` → Stored procedure for loading data into the Transformation layer.
