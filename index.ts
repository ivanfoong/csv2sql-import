import * as fs from 'fs'
import * as csv from 'csv-parser'


const CSV_FILE = 'medium-users.csv'
const OUTPUT_FILE = 'medium_users.sql'
const ENGINE: EngineType = 'postgresql'
const DATABASE_NAME = 'test'
const TABLE_NAME = 'medium_users'
const INSERT_STATEMENT_BATCH_SIZE = 1000

type EngineType = 'mysql' | 'postgresql'
type MySQLDataType = 'TEXT' | 'INT' | 'DOUBLE' | 'BIT'
type PostgreSQLDataType = 'TEXT' | 'INT' | 'DOUBLE' | 'BOOLEAN'
type SQLDataType = MySQLDataType | PostgreSQLDataType

interface ColumnType {
  column: string
  dataType: SQLDataType
  parameters?: string
}

type RowCell = string | number | boolean
type Row = RowCell[]

const parseCSV = async (filename: string, outputEngineType: EngineType): Promise<{rows: Row[], columns: ColumnType[]}> => {
  return new Promise((resolve, reject) => {
    const rows: Row[] = []
    let columns: ColumnType[] = []

    const parseSQLDataTypePostgreSQL = (value: any): SQLDataType => {
      if (typeof value === 'number') {
        return (value % 1 === 0) ? 'INT' : 'DOUBLE'
      } else if (typeof value === 'boolean') {
        return 'BOOLEAN'
      } else if (typeof value === 'string' && !isNaN(Number(value))) {
        return (Number(value) % 1 === 0) ? 'INT' : 'DOUBLE'
      } else { // handle as string for everything else
        return 'TEXT'
      }
    }
    
    const parseSQLDataTypeMySQL = (value: any): SQLDataType => {
      if (typeof value === 'number') {
        return (value % 1 === 0) ? 'INT' : 'DOUBLE' 
      } else if (typeof value === 'boolean') {
        return 'BIT'
      } else if (typeof value === 'string' && !isNaN(Number(value))) {
        return (Number(value) % 1 === 0) ? 'INT' : 'DOUBLE'
      } else { // handle as string for everything else
        return 'TEXT'
      }
    }
    
    const parseSQLDataType = (value: any, engineType: EngineType): SQLDataType => {
      if (engineType === 'postgresql') {
        return parseSQLDataTypePostgreSQL(value)
      } else if (engineType === 'mysql') {
        return parseSQLDataTypeMySQL(value)
      }
    }
    
    const parseColumns = (row: {}, engineType: EngineType): ColumnType[] => {
      return Object.keys(row).map(column => {
        const newColumn: ColumnType = {
          column,
          dataType: parseSQLDataType(row[column], engineType),
          parameters: 'NULL'
        }
        return newColumn
      })
    }
    
    const handleRow = (row: any, engineType: EngineType) => {
      if (typeof row === 'object') {
        if (columns.length === 0) {
          columns = parseColumns(row as {}, engineType)
        }
        rows.push(Object.keys(row).map(column => row[column]))
      }
    }
    
    fs.createReadStream(filename)
      .pipe(csv())
      .on('data', row => handleRow(row, outputEngineType))
      .on('end', () => {
        resolve({
          rows,
          columns,
        })
      })
  })
}

const generateSQLStatements = (rows: Row[], columns: ColumnType[], engineType: EngineType, databaseName: string, tableName: string, batchSize: number): string => {
  const generateColumnStatement = (column: ColumnType, engineType?: EngineType): string => {
    if (engineType === 'postgresql') {
      return `"${column.column}" ${column.dataType}${column.parameters ? ' ' + column.parameters : ''}`
    } else if (engineType === 'mysql') {
      return `\`${column.column}\` ${column.dataType}${column.parameters ? ' ' + column.parameters : ''}`
    } else {
      return `${column.column} ${column.dataType}${column.parameters ? ' ' + column.parameters : ''}`
    }
  }

  const generateInsertPartialStatementRow = (columns: ColumnType[], row: Row): string => {
    return `(${row.map((cell, index) => columns[index].dataType === 'TEXT' ? `'${cell}'` : cell)})`
  }

  const generateCreateStatement = (columns: ColumnType[], rows: Row[], engineType: EngineType): string => {
    if (engineType === 'postgresql') {
      const POSTGRESQL_CREATE_STATEMENT = `SELECT 'CREATE DATABASE ${databaseName}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${databaseName}')\\gexec
\\c "${databaseName}";
CREATE TABLE IF NOT EXISTS "${tableName}" (${columns.map(col => generateColumnStatement(col, 'postgresql')).join(',\n')});`
      return POSTGRESQL_CREATE_STATEMENT
    } else if (engineType === 'mysql') {
      const MYSQL_CREATE_STATEMENT = `CREATE DATABASE IF NOT EXISTS \`${databaseName}\`;
USE \`${databaseName}\`;
CREATE TABLE IF NOT EXISTS \`${tableName}\` (
  ${columns.map(col => generateColumnStatement(col, 'mysql')).join(',\n  ')}
);`
      return MYSQL_CREATE_STATEMENT
    }
  }

  const generateInsertStatements = (columns: ColumnType[], rows: Row[], batchSize: number): string => {
    const rowsClone = [...rows]
    const statements: string[] = []
    while(rowsClone.length) {
      const rowsBatch = rowsClone.splice(0, batchSize)
      const INSERT_STATEMENT = `INSERT INTO ${tableName} VALUES
  ${rowsBatch.map(row => generateInsertPartialStatementRow(columns, row)).join(',\n  ')};`
      statements.push(INSERT_STATEMENT)
    }
    return statements.join('\n')
  }

  return `${generateCreateStatement(columns, rows, engineType)}
${generateInsertStatements(columns, rows, batchSize)}`
}

const main = async (csvFile: string, sqlFile: string, engineType: EngineType, databaseName: string, tableName: string, batchSize: number) => {
  const { rows, columns } = await parseCSV(csvFile, engineType)
  console.log(`Processed CSV file '${csvFile}' for '${engineType}' database`)
  // console.log(columns)
  // console.log(rows[0])
  const statements = generateSQLStatements(rows, columns, engineType, databaseName, tableName, batchSize)
  console.log(`Generated SQL statements for ${rows.length} rows and ${columns.length} columns`)
  // console.log(statements)
  fs.writeFileSync(sqlFile, statements)
  console.log(`Written SQL statements to '${sqlFile}'`)
}

main(CSV_FILE, OUTPUT_FILE, ENGINE, DATABASE_NAME, TABLE_NAME, INSERT_STATEMENT_BATCH_SIZE)
