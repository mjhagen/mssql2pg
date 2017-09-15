component accessors=true extends="mustang.services.threaded" {
  property utilityService;
  property logService;

  this.pageSize = 5000;
  this.insertBatchSize = 250;
  this.eol = chr( 13 ) & chr( 10 );

  // CONSTRUCTOR:

  public component function init( root, progressService, beanFactory ) {
    beanFactory.declare( "sourceQueryBean" )
      .aliasFor( "queryService" )
      .asTransient( );

    beanFactory.declare( "destinationQueryBean" )
      .aliasFor( "queryService" )
      .asTransient( );

    variables.sourceQueryService = beanFactory.getBean( "sourceQueryBean", { ds = "source" } );
    variables.destinationQueryService = beanFactory.getBean( "destinationQueryBean", { ds = "destination" } );

    variables.pgKeywords = deserializeJSON( fileRead( root & "/config/postgres-keywords.json" ) );
    variables.progress = progressService.getInstance( );

    variables.items = [ ];
    variables.tables = { };
    variables.fks = [ ];

    return super.init( );
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

    initThreads( );

    variables.progress.setStatus( "QUEUE: Retrieve source database info" );
    variables.progress.addToTotal( );
    addTask( getSourceTables );

    variables.progress.setStatus( "QUEUE: Generate destination SQL" );
    variables.progress.addToTotal( );
    addTask( generateDestinationSQL );

    variables.progress.setStatus( "QUEUE: Clear destination database" );
    variables.progress.addToTotal( );
    addTask( initializeDestination );

    variables.progress.setStatus( "QUEUE: Run export" );
    variables.progress.addToTotal( );
    addTask( runTransfer );
  }

  public struct function getSyncProgress( ) {
    return progressService.getProgress( );
  }

  public struct function getInstanceVariables( ) {
    return {
      pgKeywords = variables.pgKeywords,
      progress = variables.progress,
      items = variables.items,
      tables = variables.tables,
      fks = variables.fks
    };
  }

  // THREADED:

  private void function getSourceTables( string tableName ) {
    var threadName = getThreadName( );
    try {
      var sourceTableDesign = { };
      var params = { };
      var sql = [
        "
          SELECT    tableInfo.name        AS [table],
                    syscolumns.name       AS [column],
                    systypes.name         AS [datatype],
                    syscolumns.length     AS [length],
                    syscolumns.isnullable AS [allowNulls],
                    CASE primarykeyInfo.xtype WHEN 'PK'
                      THEN 1
                      ELSE 0
                    END AS [pk]
        ",
        "
          FROM      sysobjects AS tableInfo
                    INNER JOIN syscolumns                     ON tableInfo.id = syscolumns.id
                    INNER JOIN systypes                       ON syscolumns.xtype = systypes.xtype
                    LEFT OUTER JOIN sysconstraints
                    INNER JOIN sysobjects AS primarykeyInfo   ON sysconstraints.constid = primarykeyInfo.id
                    INNER JOIN sysindexes                     ON primarykeyInfo.name = sysindexes.name AND primarykeyInfo.parent_obj = sysindexes.id
                    INNER JOIN sysindexkeys                   ON sysindexes.id = sysindexkeys.id AND sysindexes.indid = sysindexkeys.indid
                                                              ON syscolumns.id = sysindexkeys.id AND syscolumns.colid = sysindexkeys.colid
        ",
        "
          WHERE     tableInfo.name <> 'dtproperties'
            AND     tableInfo.xtype = 'U'
        ",
        "
          ORDER BY  [table], syscolumns.colorder, [column]
        "
      ];

      if ( !isNull( tableName ) && isSimpleValue( tableName ) ) {
        sql[ 3 ] &= " AND tableInfo.name = :tableName ";
        params[ "tableName" ] = tableName;
      }

      var allTables = variables.sourceQueryService.execute( arrayToList( sql, ' ' ), params );

      for ( var row in allTables ) {
        if ( !structKeyExists( sourceTableDesign, row.table ) ) {
          sourceTableDesign[ row.table ] = { "columns" = [ ] };
        }

        arrayAppend(
          sourceTableDesign[ row.table ].columns,
          {
            "name" = row.column,
            "datatype" = row.datatype,
            "length" = row.length,
            "allowNulls" = row.allowNulls,
            "pk" = row.pk
          }
        );
      }

      for ( var tableName in sourceTableDesign ) {
        var rowCounter = variables.sourceQueryService.execute( "SELECT COUNT( * ) AS total FROM [#tableName#]", { } );
        sourceTableDesign[ tableName ][ "rowCount" ] = rowCounter.total;
      }

      variables.sortedTableKeys = structSort( sourceTableDesign, "numeric", "asc", "rowCount" );

      variables.tables = sourceTableDesign;
      variables.fks = getSourceForeignKeys( );
      variables.progress.setStatus( "#structCount( sourceTableDesign )# tables found." );
    } catch ( any e ) {
      variables.logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private void function generateDestinationSQL( ) {
    var threadName = getThreadName( );
    try {
      var tmp = [ ];

      for ( var tableName in variables.sortedTableKeys ) {
        arrayAppend( tmp, getSqlInsertForTable( tableName ) );
      }

      variables.items = tmp;
      variables.progress.setStatus( "Destination SQL generated." );
    } catch ( any e ) {
      variables.logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private void function initializeDestination( ) {
    var threadName = getThreadName( );
    try {
      var sql = "
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        GRANT ALL ON SCHEMA public TO postgres;
        GRANT ALL ON SCHEMA public TO public;
        COMMENT ON SCHEMA public IS 'standard public schema';
      ";

      transaction {
        try {
          variables.destinationQueryService.execute( sql );
          transactionCommit( );
          variables.progress.setStatus( "Destination database cleared." );
        } catch ( any e ) {
          transactionRollback( );
          variables.progress.setStatus( "Error clearing destination database. (#e.detail#)" );
          rethrow;
        }
      }

      variables.progress.setStatus( "Removing old foreign keys." );
      removeOldForeignKeys( );

      variables.progress.setStatus( "Creating destination tables." );
      createDestinationTables( );

      variables.progress.setStatus( "Destination tables generated." );
    } catch ( any e ) {
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private void function runTransfer( ) {
    var threadName = getThreadName( );
    try {
      variables.progress.setStatus( "Generating batch operations" );
      var tableOperations = [ ];
      for ( var item in variables.items ) {
        tableOperations.add( getBatches( item ) );
      }

      variables.progress.setStatus( "QUEUE: Add batches to queue" );
      for ( var batches in tableOperations ) {
        for ( var batch in batches ) {
          variables.progress.addToTotal( );
          addTask( queueBatch, batch );
        }
      }

      variables.progress.setStatus( "QUEUE: Add indexes and constraints" );
      variables.progress.addToTotal( );
      addTask( addIndexesAndConstraints );

      variables.progress.setStatus( "QUEUE: Validate data" );
      variables.progress.addToTotal( );
      addTask( validateData );
    } catch ( any e ) {
      variables.logService.dumpToFile( e );
      variables.progress.setStatus( e.message );
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private void function addIndexesAndConstraints( ) {
    var threadName = getThreadName( );
    try {
      variables.progress.setStatus( "Adding indexes and constraints." );

      for ( var row in variables.fks ) {
        var removeNonExisting = "
          DELETE
          FROM #escapePgKeyword( row.from_table )#
          WHERE NOT EXISTS (
            SELECT #escapePgKeyword( row.to_table )#.*
            FROM #escapePgKeyword( row.to_table )#
            WHERE #escapePgKeyword( row.to_table )#.#row.to_column# = #row.from_table#.#row.from_column#
          );
        ";

        var debugInfo = variables.destinationQueryService.execute( "EXPLAIN " & removeNonExisting );
        variables.progress.setStatus( "Removal impact: " & debugInfo[ "QUERY PLAN" ][ 1 ] );

        transaction {
          try {
            variables.progress.setStatus( "#row.to_table# - #row.from_table#: Removing FK mismatched rows" );
            variables.destinationQueryService.execute( removeNonExisting );
            transactionCommit( );
          } catch ( any e ) {
            transactionRollback( );
          }
        }

        var fkSql = "
          ALTER TABLE IF EXISTS #escapePgKeyword( row.to_table )#
            ADD CONSTRAINT #row.name#
              FOREIGN KEY ( #row.from_column# )
              REFERENCES #escapePgKeyword( row.to_table )# (#row.to_column#)
              MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;
        ";

        transaction {
          try {
            variables.progress.setStatus( "#row.to_table# - #row.from_table#: Adding indexes" );
            variables.destinationQueryService.execute( fkSql );
            transactionCommit( );
          } catch ( any e ) {
            transactionRollback( );
          }
        }
      }

      variables.progress.setStatus( "Indexes and constraints added." );
    } catch ( any e ) {
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private array function getBatches( required struct input, numeric maxRows = 0 ) {
    var result = [ ];

    var inputSize = input.data.len( );

    if ( maxRows > 0 ) {
      inputSize = maxRows;
    }

    var batchSize = min( this.insertBatchSize, inputSize );

    if ( batchSize > 0 ) {
      var offset = 1;
      var batchNr = 1;
      var noSteps = ceiling( inputSize / batchSize );
      var currentStep = 1;

      do {
        var slice = ( offset + batchSize > inputSize )
          ? min( batchSize, inputSize - ( offset - 1 ) )
          : batchSize;

        var batch = input.data.slice( offset, slice );
        var paramsSlice = input.params.slice( offset, slice );
        var params = [ ];

        for ( var paramsPerRow in paramsSlice ) {
          params.addAll( paramsPerRow );
        }

        result.add(
          {
            "sql" = input.insert & this.eol & batch.toList( ',' & this.eol ) & ';',
            "params" = params,
            "batchNr" = batchNr
          }
        );

        offset += ( batchSize );
        batchNr++;
      } while ( !arrayIsEmpty( batch ) && offset < inputSize );
    }

    return result;
  }

  private void function queueBatch( required string sql, required array params, required numeric batchNr ) {
    var threadName = getThreadName( );
    try {
      transaction {
        try {
          variables.destinationQueryService.execute( sql, params );
          transactionCommit( );
          variables.progress.setStatus( "Batch #batchNr# done." );
        } catch ( any e ) {
          transactionRollback( );
          rethrow;
        }
      }
    } catch ( any e ) {
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private void function validateData( ) {
    var threadName = getThreadName( );
    try {
      for ( var tableName in variables.sortedTableKeys ) {
        var destinationRows = 0;

        try {
          var rowCounter = variables.destinationQueryService.execute(
            "SELECT COUNT( * ) AS total FROM #escapePgKeyword( tableName )#"
          );
          destinationRows = rowCounter.total;
        } catch ( any e ) {
          variables.progress.setStatus( "VALIDATION: Error in #tableName#: #e.message#" );
        }

        var sourceTable = variables.tables[ tableName ];

        if ( sourceTable.rowCount != destinationRows ) {
          variables.progress.setStatus( "VALIDATION: #tableName# ERROR: difference in row count" );
        }
      }

      variables.progress.setStatus( "QUEUE: Done" );
      variables.progress.addToTotal( );
      addTask( transferDone );
    } catch ( any e ) {
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );
    }
  }

  private void function transferDone( ) {
    // see if all threads are completed:
    while ( getNumberOfRunningThreads( ) ) {
      variables.progress.setStatus( "Waiting on threads" );
      variables.logService.writeLogLevel( "Threads still running... sleeping for 0.5 seconds." );
      sleep( 500 );
    }

    variables.progress.setStatus( "Transfer done" );
    variables.progress.done( );
  }

  // HELPER METHODS:

  private struct function getSqlInsertForTable( required string tableName ) {
    var columns = variables.tables[ tableName ].columns;
    var escapedColumns = [ ];

    for ( var column in columns ) {
      escapedColumns.add( escapePgKeyword( column.name ) );
    }

    escapedColumns = escapedColumns.toList( );

    var sourceData = getSourceData( tableName );
    var numberOfRows = sourceData.recordCount;

    variables.progress.setStatus( "Done retrieving records from '#tableName#'." );

    var pages = [ ];
    var rowNr = 0;
    var currentPage = 1;
    var questionMarks = [ ];
    var numberOfColumns = columns.len( );
    questionMarks.set( 1, numberOfColumns, "?" );
    questionMarks = questionMarks.toList( );

    variables.progress.setStatus( "Formatting data for destination." );

    for ( var row in sourceData ) {
      rowNr++;

      if ( rowNr MOD this.pageSize == 0 ) {
        currentPage++;
        variables.progress.setStatus( "Formatting rows #rowNr#/#numberOfRows#." );
      }

      if ( !arrayIsDefined( pages, currentPage ) ) {
        pages[ currentPage ] = { sql = [ ], params = [ ] };
      }

      var paramsPerRow = [ ];

      for ( var column in columns ) {
        var value = row[ column.name ];
        paramsPerRow.add( formatValueForPostgres( value, column ) );
      }

      pages[ currentPage ].sql.add( "( #questionMarks# )" );
      pages[ currentPage ].params.add( paramsPerRow );
    }

    var sql = [ ];
    var params = [ ];
    var numberOfPages = pages.len( );

    for ( var i = 1; i <= numberOfPages; i++ ) {
      sql.addAll( pages[ i ].sql );
      params.addAll( pages[ i ].params );
    }

    var result = {
      "table" = tableName,
      "insert" = 'INSERT INTO "public"."#tableName#" ( #escapedColumns# ) VALUES ',
      "data" = sql,
      "params" = params
    };

    variables.progress.setStatus( "'#tableName#' Done. (#result.data.len( )#)" );

    return result;
  }

  private query function getSourceData( required string tableName ) {
    cfsetting( requesttimeout = 300 );

    var columns = variables.tables[ tableName ].columns;
    var mssqlColumnList = [ ];

    for ( var column in columns ) {
      arrayAppend( mssqlColumnList, "[#column.name#]" );
    }

    var rowCount = variables.tables[ tableName ].rowCount;

    variables.progress.setStatus( "Retrieving #rowCount# row(s) from '#tableName#'..." );

    var result = variables.sourceQueryService.execute(
      "SELECT #arrayToList( mssqlColumnList )# FROM [#tableName#]",
      { }
    );

    return result;
  }

  private string function createDestinationTables( ) {
    transaction {
      try {
        for ( var tableName in variables.sortedTableKeys ) {
          var table = variables.tables[ tableName ].columns;
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

          var sql = "CREATE TABLE public.#escapePgKeyword( tableName )# ( #columns.toList( )# );";

          variables.destinationQueryService.execute( sql );

          variables.progress.setStatus( "Table '#tableName#' created." );
        }
        transactionCommit( );
        variables.progress.setStatus( "All tables created." );
      } catch ( any e ) {
        transactionRollback( );
        rethrow;
      }
    }
  }

  private query function getSourceForeignKeys( ) {
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

    return variables.sourceQueryService.execute( sql );
  }

  private void function removeOldForeignKeys( ) {
    for ( var row in variables.fks ) {
      try {
        variables.destinationQueryService.execute(
          "ALTER TABLE IF EXISTS #escapePgKeyword( row.from_table )# DROP CONSTRAINT IF EXISTS #row.name#;"
        );
        variables.progress.setStatus( "Removed FK '#row.name#'" );
      } catch ( any e ) {
      }
    }
  }

  private struct function formatValueForPostgres( required any value, required struct md ) {
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

  private string function asCFDatatype( required string datatype ) {
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
      case "text":
        return "longvarchar";
    }

    return datatype;
  }

  private string function asPostgresDatatype( required string datatype ) {
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

  private boolean function hasLength( required struct column ) {
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

  private string function escapePgKeyword( required string keyword ) {
    if ( arrayFindNoCase( variables.pgKeywords, keyword ) ) {
      return '"#lCase( keyword )#"';
    }

    return keyword;
  }
}