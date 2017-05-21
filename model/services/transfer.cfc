component extends="mustang.services.threaded" {
  public component function init( root, progressService ) {
    variables.sourceOptions = { datasource = "source" };
    variables.destinationOptions = { datasource = "destination" };

    variables.eol = chr( 13 ) & chr( 10 );

    variables.pgKeywords = deserializeJson( fileRead( root & "/config/postgres-keywords.json" ) );
    variables.progress = progressService.getInstance( );

    variables.items = [ ];
    variables.tables = { };
    variables.fks = [ ];

    return super.init();
  }

  // PUBLIC API:
  public void function start( ) {
    prevProgress = progressService.getInstance( structKeyExists( url, "reload" ) );
    prevProgress.done( );

    if ( !prevProgress.getDone( ) ) {
      throw( "Transfer still running", "transferService.start.stillRunningError" );
    }

    variables.progress = progressService.getInstance( true );
    variables.progress.setDone( false );

    variables.progress.setStatus( "QUEUE: Retrieve source database info" );
    addTask( getSourceTables );

    variables.progress.setStatus( "QUEUE: Generate destination SQL" );
    addTask( generateDestinationSQL );

    variables.progress.setStatus( "QUEUE: Clear destination database" );
    addTask( initializeDestination );

    variables.progress.setStatus( "QUEUE: Run export" );
    addTask( runTransfer );

    variables.progress.setStatus( "QUEUE: Add indexes and constraints" );
    addTask( addIndexesAndConstraints );
  }

  public struct function getSyncProgress() {
    return progressService.getProgress( );
  }

  public struct function getInstanceVariables() {
    return {
      destinationOptions = variables.destinationOptions,
      eol = variables.eol,
      items = variables.items,
      pgKeywords = variables.pgKeywords,
      progress = variables.progress,
      sourceOptions = variables.sourceOptions,
      tables = variables.tables
    };
  }

  // Actual transfer functions:

  public void function getSourceTables( string tableName ) {
    try {
      var params = { };
      var sql = [ "
          SELECT    tableInfo.name        AS [table],
                    syscolumns.name       AS [column],
                    datatypeInfo.name     AS [datatype],
                    syscolumns.length     AS [length],
                    syscolumns.isnullable AS [allowNulls],
                    CASE primarykeyInfo.xtype WHEN 'PK'
                      THEN 1
                      ELSE 0
                    END AS [pk]
        ", "
          FROM      sysobjects AS tableInfo
                    INNER JOIN syscolumns                     ON tableInfo.id = syscolumns.id
                    INNER JOIN systypes AS datatypeInfo       ON syscolumns.xtype = datatypeInfo.xtype
                    LEFT OUTER JOIN sysconstraints
                    INNER JOIN sysobjects AS primarykeyInfo   ON sysconstraints.constid = primarykeyInfo.id
                    INNER JOIN sysindexes                     ON primarykeyInfo.name = sysindexes.name AND primarykeyInfo.parent_obj = sysindexes.id
                    INNER JOIN sysindexkeys                   ON sysindexes.id = sysindexkeys.id AND sysindexes.indid = sysindexkeys.indid
                                                              ON syscolumns.id = sysindexkeys.id AND syscolumns.colid = sysindexkeys.colid
        ", "
          WHERE     tableInfo.name <> 'dtproperties'
            AND     tableInfo.xtype = 'U'
        ", "
          ORDER BY  [table], [column]
        "
      ];

      if ( !isNull( tableName ) && isSimpleValue( tableName ) ) {
        sql[ 3 ] &= " AND tableInfo.name = :tableName ";
        params[ "tableName" ] = tableName;
      }

      var allTables = queryExecute( arrayToList( sql, ' ' ), params, sourceOptions );
      var tmp = { };

      for ( var row in allTables ) {
        if ( !structKeyExists( tmp, row.table ) ) {
          tmp[ row.table ] = [ ];
        }

        arrayAppend( tmp[ row.table ], {
          "name" = row.column,
          "datatype" = row.datatype,
          "length" = row.length,
          "allowNulls" = row.allowNulls,
          "pk" = row.pk
        } );
      }

      variables.tables = tmp;
      variables.fks = getSourceForeignKeys( );
      variables.progress.setStatus( "#structCount( tmp )# tables found." );
    } catch ( any e ) {
      logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      rethrow;
    }
  }

  public query function getSourceData( tableName ) {
    var table = variables.tables[ tableName ];
    var mssqlColumnList = [ ];

    for ( var column in table ) {
      arrayAppend( mssqlColumnList, "[#column.name#]" );
    }

    return queryExecute( "SELECT #arrayToList( mssqlColumnList )# FROM [#tableName#]", { }, sourceOptions );
  }

  public void function generateDestinationSQL( ) {
    try {
      var tmp = [ ];

      var counter = 0;
      for ( var tableName in variables.tables ) {
        // if ( counter++ < 10 ) { }

        arrayAppend( tmp, getSqlInsertForTable( tableName ) );
        variables.progress.addToTotal( );
      }

      variables.items = tmp;

      variables.progress.setStatus( "Destination SQL generated." );
    } catch ( any e ) {
      logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      rethrow;
    }
  }

  public void function initializeDestination( ) {
    var sql = "
      DROP SCHEMA public CASCADE;
      CREATE SCHEMA public;
      GRANT ALL ON SCHEMA public TO postgres;
      GRANT ALL ON SCHEMA public TO public;
      COMMENT ON SCHEMA public IS 'standard public schema';
    ";

    transaction {
      try {
        queryExecute( sql, { }, destinationOptions );
        transactionCommit( );
        variables.progress.setStatus( "Destination database cleared." );
      } catch ( any e ) {
        transactionRollback( );
        variables.progress.setStatus( "Error clearing destination database. (#e.detail#)" );
        rethrow;
      }
    }

    createDestinationTables( );
    removeOldForeignKeys();

    variables.progress.setStatus( "Destination tables generated." );
  }

  public void function runTransfer( ) {
    try {
      var counter = 0;
      for ( var item in variables.items ) {
        variables.progress.setStatus( "QUEUE: Task #++counter#" );
        addTask( transferTable, { input = item } );
      }
    } catch ( any e ) {
      logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      rethrow;
    }
  }

  public void function transferTable( required struct input, numeric maxRows = 0 ) {
    try {
      cfsetting( requesttimeout = 1200 );

      transaction {
        try {
          variables.progress.setStatus( "Transferring #input.table# records..." );

          var inputSize = input.data.len( );

          if ( maxRows > 0 ) {
            inputSize = maxRows;
          }

          var batchSize = min( 250, inputSize );
          var offset = 1;
          var noSteps = ceiling(inputSize/batchSize);
          var currentStep = 1;

          do {
            variables.progress.setStatus( "#input.table# - Batch: #offset# (#currentStep++#/#noSteps#)" );

            var slice = ( offset + batchSize > inputSize )
              ? min( batchSize, inputSize - ( offset - 1 ) )
              : batchSize;

            var batch = input.data.slice( offset, slice );
            var sql = input.insert & eol & arrayToList( batch, ',' & eol ) & ';';

            var paramsSlice = input.params.slice( offset, slice );
            var params = [ ];
            for ( var paramsPerRow in paramsSlice ) {
              params.addAll( paramsPerRow );
            }

            queryExecute( sql, params, destinationOptions );

            offset += ( batchSize );

          } while ( arrayLen( batch ) && offset < inputSize );

          transactionCommit( );
        } catch ( any e ) {
          variables.progress.setStatus( e.detail );
          transactionRollback( );
          rethrow;
        }
      }
    } catch ( any e ) {
      logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
    }
  }

  public void function transferDone( ) {
    variables.progress.setStatus( "Transfer done" );
    variables.progress.done( );
  }

  public string function createDestinationTables( ) {
    transaction {
      try {
        for ( var tableName in variables.tables ) {
          var table = variables.tables[ tableName ];
          var columns = [ ];

          for ( var column in table ) {
            var sqlColumnDefinition = escapePgKeyword( column.name );

            sqlColumnDefinition &= ' #asPostgresDatatype( column.datatype )#';

            if ( hasLength( column ) ) {
              sqlColumnDefinition &= ' (#column.length#)';
            }

            if ( !column.allowNulls ) {
              sqlColumnDefinition &= ' NOT NULL';
            }

            if ( column.pk ) {
              sqlColumnDefinition &= ' PRIMARY KEY';
            }

            arrayAppend( columns, sqlColumnDefinition );
          }

          var sql = 'CREATE TABLE public.#escapePgKeyword( tableName )# (' & arrayToList( columns ) & ');';

          queryExecute( sql, { }, destinationOptions );

          variables.progress.setStatus( "Table '#tableName#' created." );
        }
        transactionCommit( );
      } catch ( any e ) {
        transactionRollback( );
        rethrow;
      }
    }
  }

  public struct function getSqlInsertForTable( tableName ) {
    var table = variables.tables[ tableName ];

    var escapedColumns = "";
    for ( var column in table ) {
      escapedColumns = listAppend( escapedColumns, escapePgKeyword( column.name ) );
    }

    var questionMarks = [ ];
    arraySet( questionMarks, 1, arrayLen( table ), "?" );
    questionMarks = arrayToList( questionMarks );

    var sourceData = getSourceData( tableName );
    var sql = [ ];
    var params = [ ];
    var rowNr = 0;

    for ( var row in sourceData ) {
      var paramsPerRow = [ ];
      rowNr++;

      for ( var column in table ) {
        var value = row[ column.name ];
        arrayAppend( paramsPerRow, formatValueForPostgres( value, column ) );
      }

      arrayAppend( sql, "( #questionMarks# )" );
      arrayAppend( params, paramsPerRow );
    }

    var result = {
      "table" = tableName,
      "insert" = 'INSERT INTO "public"."#tableName#" ( #escapedColumns# ) VALUES ',
      "data" = sql,
      "params" = params
    };

    variables.progress.setStatus( "#sourceData.recordCount# records in tables '#tableName#'." );

    return result;
  }

  public query function getSourceForeignKeys( ) {
    var sql = "
      SELECT    so.name                   AS [name],
                object_name( sf.fkeyid )  AS [from_table],
                sc1.name                  AS [from_column],
                object_name( sf.rkeyid )  AS [to_table],
                sc2.name                  AS [to_column]

      FROM      sysobjects so
                INNER JOIN sysforeignkeys sf ON so.id = sf.constid
                INNER JOIN syscolumns sc1 ON sf.fkeyid = sc1.id AND sf.fkey = sc1.colid
                INNER JOIN syscolumns sc2 ON sf.rkeyid = sc2.id AND sf.rkey = sc2.colid

      WHERE     so.xtype IN ( 'f', 'pk' )

      ORDER BY  [from_table], [from_column]
    ";

    return queryExecute( sql, { }, variables.sourceOptions );
  }

  public void function removeOldForeignKeys( ) {
    for ( var row in variables.fks ) {
      transaction {
        try {
          queryExecute( "ALTER TABLE #escapePgKeyword( row.from_table )# DROP CONSTRAINT IF EXISTS #row.name#;", { }, variables.destinationOptions );
          transactionCommit( );
          variables.progress.setStatus( "Removed FK '#row.name#'" );
        } catch ( any e ) {
          transactionRollback( );
          variables.progress.setStatus( "Error removed FK '#row.name#' (#e.detail#)" );
        }
      }
    }
  }

  public void function addIndexesAndConstraints( ) {
    // TODO: add indexes and constraints...

    for ( var row in variables.fks ) {
      // var removeNonExisting = "DELETE FROM #escapePgKeyword( row.from_table )# WHERE NOT EXISTS ( SELECT #escapePgKeyword( row.to_table )#.* FROM #escapePgKeyword( row.to_table )# WHERE #escapePgKeyword( row.to_table )#.#row.to_column# = #row.from_table#.#row.from_column# );";
      var dropFk = "ALTER TABLE #escapePgKeyword( row.from_table )# DROP CONSTRAINT IF EXISTS #row.name#;";
      var fkSql = "ALTER TABLE #escapePgKeyword( row.from_table )# ADD CONSTRAINT #row.name# FOREIGN KEY ( #row.from_column# ) REFERENCES #escapePgKeyword( row.to_table )# (#row.to_column#) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;";

      try {
        // queryExecute( removeNonExisting, { }, variables.destinationOptions );
        queryExecute( fkSql, { }, variables.destinationOptions );
      } catch ( any e ) {
      }
    }

    variables.progress.setStatus( "QUEUE: Done" );
    addTask( transferDone );
  }

  // public (helpers):

  public struct function formatValueForPostgres( value, md ) {
    var result = {
      value = value,
      cfsqltype = asCFDatatype( md.datatype ),
      null = false
    };

    if ( !isSimpleValue( value ) || ( !len( trim( value ) ) && md.allownulls ) ) {
      result.null = true;
      return result;
    }

    switch ( md.datatype ) {
      case "tinyint":
      case "boolean":
        result.value = createObject( "java", "java.lang.Boolean" ).init( !!value );
        return result;

      case "date":
      case "time":
      case "timestamp":
      case "smalldatetime":
      case "datetime":
        result.value = createODBCDateTime( value );
        return result;
    }

    return result;
  }

  public string function asCFDatatype( datatype ) {
    switch ( datatype ) {
      case "int":
        return "integer";
      case "tinyint":
      case "boolean":
        return "bit";
      case "date":
      case "time":
      case "timestamp":
      case "smalldatetime":
      case "datetime":
        return "timestamp";
    }

    return datatype;
  }

  public string function asPostgresDatatype( datatype ) {
    switch ( datatype ) {
      case "date":
      case "time":
      case "datetime":
      case "smalldatetime":
        return "timestamp";
      case "bit":
      case "tinyint":
        return "boolean";
    }

    return datatype;
  }

  public boolean function hasLength( required struct column ) {
    switch ( column.datatype ) {
      case "int":
      case "bit":
      case "integer":
      case "smallint":
      case "bigint":
      case "tinyint":
      case "boolean":
      case "datetime":
      case "smalldatetime":
      case "text":
        return false;
    }

    return column.length > 0;
  }

  public string function escapePgKeyword( required string keyword ) {
    if ( arrayFindNoCase( variables.pgKeywords, keyword ) ) {
      return '"#lCase( keyword )#"';
    }

    return keyword;
  }
}