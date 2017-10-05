component accessors=true extends="mustang.services.threaded" {
  property logService;
  property progressService;

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

    variables.items = { };
    variables.tables = { };
    variables.fks = [ ];

    structAppend( variables, arguments );

    return super.init( );
  }

  // PUBLIC API:

  public void function start( boolean runSingleThreaded = false, boolean skipTransfer = false ) {
    variables.runSingleThreaded = runSingleThreaded;

    prevProgress = variables.progressService.getInstance( structKeyExists( url, "reload" ) );
    prevProgress.done( );

    if ( !prevProgress.getDone( ) ) {
      throw( "Transfer still running", "transferService.start.stillRunningError" );
    }

    variables.progress = variables.progressService.getInstance( true );
    variables.progress.setDone( false );

    initThreads( );

    variables.progress.setStatus( "QUEUE: Retrieve source database info" );
    variables.progress.addToTotal( );
    addTask( getSourceTables );

    if ( !skipTransfer ) {
      variables.progress.setStatus( "QUEUE: Initialize destination database" );
      variables.progress.addToTotal( );
      addTask( initializeDestination );

      variables.progress.setStatus( "QUEUE: Generate destination SQL" );
      variables.progress.addToTotal( );
      addTask( runTransfer );
    }

    variables.progress.setStatus( "QUEUE: Add FKs, indexes and finish up" );
    variables.progress.addToTotal( );
    addTask( postProcessing );
  }

  public struct function getSyncProgress( ) {
    return variables.progressService.getProgress( );
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
            AND     tableInfo.name <> 'sysdiagrams'
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

      var allTables = variables.sourceQueryService.execute( sql.toList( ' ' ), params );

      for ( var row in allTables ) {
        if ( !sourceTableDesign.keyExists( row.table ) ) {
          sourceTableDesign[ row.table ] = { "columns" = [ ] };
        }

        sourceTableDesign[ row.table ].columns.add(
          {
            "name" = row.column,
            "datatype" = row.datatype,
            "length" = row.length,
            "allowNulls" = row.allowNulls > 0,
            "pk" = row.pk > 0
          }
        );
      }

      for ( var tableName in sourceTableDesign ) {
        var rowCounter = variables.sourceQueryService.execute( "SELECT COUNT( * ) AS total FROM [#tableName#]", { } );
        sourceTableDesign[ tableName ][ "rowCount" ] = rowCounter.total;
      }

      variables.sortedTableNames = sourceTableDesign.sort( "numeric", "asc", "rowCount" );

      // var tmp = [
      //   variables.sortedTableNames[ 10 ],
      //   variables.sortedTableNames[ 31 ],
      //   variables.sortedTableNames[ 25 ],
      //   variables.sortedTableNames[ 41 ]
      // ];

      // variables.sortedTableNames = tmp;

      variables.tables = sourceTableDesign;
      variables.fks = getSourceForeignKeys( );
      variables.progress.setStatus( "#sourceTableDesign.count( )# tables found." );
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

  private void function runTransfer( ) {
    var threadName = getThreadName( );
    try {
      var variables.items = { };

      for ( var tableName in variables.sortedTableNames ) {
        variables.items[ tableName ] = getSqlInsertForTable( tableName );

        var batches = getBatches( tableName );

        for ( var batch in batches ) {
          variables.progress.setStatus( "QUEUE: '#tableName#' batch #batch.batchNr# queued" );
          variables.progress.addToTotal( );
          addTask( executeBatch, { "batch" = batch } );
        }

        variables.progress.setStatus( "QUEUE: '#tableName#' batch cleanup queued" );
        variables.progress.addToTotal( );
        addTask( clearBatches, { "tableName" = tableName } );
      }

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

    variables.progress.setStatus( "Initializing destination database." );

    try {
      transaction {
        try {
          clearDestinationDatabase( );
          createDestinationTables( );
          transactionCommit( );
        } catch ( any e ) {
          transactionRollback( );
          variables.progress.setStatus( "Error clearing destination database. (#e.detail#)" );
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

  private void function postProcessing( ) {
    var threadName = getThreadName( );
    try {
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

      transaction {
        removeOldForeignKeys( );
        transactionCommit( );
      }

      for ( var row in variables.fks ) {
        transaction {
          addIndexes( row );
          transactionCommit( );
        }

        transaction {
          removeNonExisting( row );
          addForeignKeys( row );
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

  private array function getBatches( required string tableName ) {
    variables.progress.setStatus( "#tableName#: Generating batches..." );

    var input =  variables.items[ tableName ];
    var result = [ ];
    var inputSize = input.data.len( );
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
            "table" = input.table,
            "batchNr" = batchNr
          }
        );

        offset += ( batchSize );
        batchNr++;
      } while ( !arrayIsEmpty( batch ) && offset < inputSize );
    }

    return result;
  }

  private void function executeBatch( required struct batch ) {
    var threadName = getThreadName( );
    try {
      transaction {
        try {
          variables.progress.setStatus( "#batch.table#: Running batch #batch.batchNr#..." );
          variables.destinationQueryService.execute( batch.sql, batch.params );
          transactionCommit( );
          variables.progress.setStatus( "#batch.table#: Batch #batch.batchNr# done." );
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

  private void function clearBatches( required string tableName ) {
    var threadName = getThreadName( );
    try {
      structDelete( variables.items, tableName );
      variables.progress.setStatus( "#tableName# batch cleared" );
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

    variables.progress.setStatus( "Validating data" );

    try {
      for ( var tableName in variables.sortedTableNames ) {
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
      variables.progress.setStatus( "Validating done" );
    } catch ( any e ) {
      variables.progress.done( );
      rethrow;
    } finally {
      variables.progress.updateProgress( );
      cleanUpThread( threadName );

      variables.progress.setStatus( "QUEUE: Done" );
      variables.progress.addToTotal( );
      addTask( transferDone );
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
    variables.progress.setStatus( "#tableName#: Formatting data..." );

    var columns = variables.tables[ tableName ].columns;
    var sourceData = getSourceData( tableName );
    var numberOfRows = sourceData.recordCount;
    var pages = [ ];
    var rowNr = 0;
    var currentPage = 1;

    for ( var row in sourceData ) {
      rowNr++;

      if ( rowNr MOD this.pageSize == 0 ) {
        currentPage++;
        variables.progress.setStatus( "#tableName#: Formatting rows #rowNr#/#numberOfRows#..." );
      }

      if ( !arrayIsDefined( pages, currentPage ) ) {
        pages[ currentPage ] = { sql = [ ], params = [ ] };
      }

      var paramsPerRow = [ ];

      for ( var column in columns ) {
        var value = row[ column.name ];
        paramsPerRow.add( formatValueForPostgres( value, column ) );
      }

      pages[ currentPage ].sql.add( "( #getParamPlaceholders( columns )# )" );
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
      "insert" = 'INSERT INTO #escapePgKeyword( tableName )# ( #toEscapedColumnList( columns )# ) VALUES ',
      "data" = sql,
      "params" = params
    };

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

    variables.progress.setStatus( "#tableName#: Retrieving #rowCount# row(s)..." );

    return variables.sourceQueryService.execute( "SELECT #arrayToList( mssqlColumnList )# FROM [#tableName#]" );
  }

  private string function createDestinationTables( ) {
    try {
      for ( var tableName in variables.sortedTableNames ) {
        var table = variables.tables[ tableName ].columns;
        var columns = [ ];

        for ( var column in table ) {
          var sqlColumnDefinition = escapePgKeyword( column.name );

          sqlColumnDefinition &= ' #asPostgresDatatype( column.datatype )#';

          if ( hasLengthProperty( column ) ) {
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
        AND     object_name( sf.fkeyid ) IN ( :tables )
        AND     object_name( sf.rkeyid ) IN ( :tables )

      ORDER BY  [from_table], [from_column]
    ";

    return variables.sourceQueryService.execute( sql, { "tables" = { value = variables.sortedTableNames, list = true } } );
  }

  private void function clearDestinationDatabase( ) {
    variables.progress.setStatus( "Clearing destination database." );
    variables.destinationQueryService.execute( "
      DROP SCHEMA public CASCADE;
      CREATE SCHEMA public;
      GRANT ALL ON SCHEMA public TO postgres;
      GRANT ALL ON SCHEMA public TO public;
      COMMENT ON SCHEMA public IS 'standard public schema';
    " );
    variables.progress.setStatus( "Destination database cleared." );
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
    if ( !isSimpleValue( value ) ) {
      variables.logService.dumpToFile( arguments );
      throw( "Not a simple value" );
    }

    var result = {
      "value" = value,
      "cfsqltype" = asCFDatatype( md.datatype )
    };

    if ( !len( trim( value ) ) ) {
      if ( md.allowNulls ) {
        result[ "null" ] = true;
        // structDelete( result, "value" );
        return result;
      } else {
        throw( "#column.name#: Empty cell, does not allow nulls." );
      }
    }

    switch ( md.datatype ) {
      case "tinyint":
      case "boolean":
        result.value = createObject( "java", "java.lang.Boolean" ).init( !!value );
        break;

      case "date":
      case "time":
      case "timestamp":
      case "smalldatetime":
      case "datetime":
        result.value = createODBCDateTime( value );
        break;

      case "int":
        result.value = javaCast( "int", value );
        break;
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

      // bigint
      // char
      // blob
      // clob
      // date
      // decimal
      // double
      // float
      // idstamp
      // longnvarchar
      // money
      // money4
      // numeric
      // real
      // refcursor
      // smallint
      // time
      // tinyint
      // varchar
      // nvarchar
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

  private boolean function hasLengthProperty( required struct column ) {
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

  private string function toEscapedColumnList( columns ) {
    var result = [ ];

    for ( var column in columns ) {
      result.add( escapePgKeyword( column.name ) );
    }

    return result.toList( );
  }

  private void function addIndexes( required struct row ) {
    var sql = 'CREATE INDEX IDX_#replace( createUUID(), '-', '', 'all' )# ON #escapePgKeyword( row.from_table )# ( #escapePgKeyword( row.from_column )# );';
    variables.destinationQueryService.execute( sql );
  }

  private void function removeNonExisting( required struct row ) {
    if ( "#row.from_table#.#row.from_column#" == "#row.to_table#.#row.to_column#" ) {
      return;
    }

    var sql = '
      FROM    #row.from_table# l
      WHERE   NOT l.#row.from_column# IS NULL
        AND   NOT EXISTS
              (
                SELECT  NULL
                FROM    #row.to_table# r
                WHERE   r.#row.to_column# = l.#row.from_column#
              )
    ';

    variables.progress.setStatus( "FK: #row.from_table# -> #row.to_table# - Counting rows without matching PK..." );
    var numberOfRowsToDelete = variables.destinationQueryService.execute( 'SELECT COUNT ( * ) AS total ' & sql );

    if ( numberOfRowsToDelete[ "total" ][ 1 ] > 0 ) {
      variables.progress.setStatus( "FK: #row.from_table# -> #row.to_table# - Removing #numberOfRowsToDelete[ "total" ][ 1 ]# row(s) from '#row.from_table#.#row.from_column#' missing corresponding '#row.to_table#.#row.to_column#'." );
      variables.destinationQueryService.execute( 'DELETE ' & sql );
    }
  }

  private void function addForeignKeys( required struct row ) {
    if ( "#row.from_table#.#row.from_column#" == "#row.to_table#.#row.to_column#" ) {
      return;
    }

    var sql = "
      ALTER TABLE IF EXISTS #escapePgKeyword( row.from_table )#
        ADD CONSTRAINT #row.name# FOREIGN KEY ( #row.from_column# )
          REFERENCES #escapePgKeyword( row.to_table )#( #row.to_column# )
          MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;
    ";

    variables.progress.setStatus( "FK: #row.from_table# -> #row.to_table# - Adding FK #row.name#" );
    variables.destinationQueryService.execute( sql );
  }

  private string function getParamPlaceholders( columns ) {
    var result = [ ];
    var numberOfColumns = columns.len( );
    result.set( 1, numberOfColumns, "?" );
    return result.toList( );
  }
}