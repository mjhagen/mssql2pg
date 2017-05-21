component extends="tests.mustang-testKit" {
  variables.transferService = beanFactory.getBean( "transferService" );

  public void function run( ) {
    describe( "Unit tests", function() {
      it( "Expects addIndexesAndConstraints to work", function() {
        transferService.addIndexesAndConstraints( );
      } );

      xit( "Expects asCFDatatype to work", function() {
        expect( transferService.asCFDatatype( "int" ) ).toBe( "integer" );
        expect( transferService.asCFDatatype( "tinyint" ) ).toBe( "bit" );
        expect( transferService.asCFDatatype( "boolean" ) ).toBe( "bit" );
        expect( transferService.asCFDatatype( "date" ) ).toBe( "timestamp" );
        expect( transferService.asCFDatatype( "time" ) ).toBe( "timestamp" );
        expect( transferService.asCFDatatype( "timestamp" ) ).toBe( "timestamp" );
        expect( transferService.asCFDatatype( "smalldatetime" ) ).toBe( "timestamp" );
        expect( transferService.asCFDatatype( "datetime" ) ).toBe( "timestamp" );
      } );

      xit( "Expects formatValueForPostgres to work", function() {
        var value = "";
        var md = { datatype = "", allownulls = false };
        var result = transferService.formatValueForPostgres( value, md );
        expect( result ).toBeTypeOf( "struct" );

        // nulls
        var md = { datatype = "", allownulls = true };
        var result = transferService.formatValueForPostgres( value, md );
        expect( result.null ).toBeTrue( );

        // boolean types
        var datatypes = listToArray( "tinyint,boolean" );
        for ( var datatype in datatypes ) {
          md.datatype = datatype;
          value = false;
          var result = transferService.formatValueForPostgres( value, md );
          expect( result.value ).toBeTypeOf( "boolean" );
        }

        // date/time types
        var datatypes = listToArray( "date,time,timestamp,smalldatetime,datetime" );
        for ( var datatype in datatypes ) {
          md.datatype = datatype;
          value = createDate( 1978, 11, 15 );
          var result = transferService.formatValueForPostgres( value, md );
          expect( result.value ).toBeTypeOf( "datetime_object" );
        }
      } );

      xit( "Expects asPostgresDatatype to work", function() {
        expect( transferService.asPostgresDatatype( "date" ) ).toBe( "timestamp" );
        expect( transferService.asPostgresDatatype( "time" ) ).toBe( "timestamp" );
        expect( transferService.asPostgresDatatype( "datetime" ) ).toBe( "timestamp" );
        expect( transferService.asPostgresDatatype( "smalldatetime" ) ).toBe( "timestamp" );
        expect( transferService.asPostgresDatatype( "tinyint" ) ).toBe( "boolean" );
      } );

      xit( "Expects hasLength to work", function() {
        var datatypes = "int,integer,smallint,bigint,tinyint,datetime,smalldatetime,text";

        for ( var datatype in datatypes ) {
          var result = transferService.hasLength( { datatype = datatype } );
          expect( result ).toBeFalse( );
        }

        var result = transferService.hasLength( { datatype = "anything else", length = 10 } );
        expect( result ).toBeTrue( );
      } );

      xit( "Expects escapePgKeyword to work", function() {
        expect( transferService.escapePgKeyword( 'table' ) ).toBeWithCase( '"table"' );
        expect( transferService.escapePgKeyword( 'table' ) ).notToBeWithCase( 'table' );
        expect( transferService.escapePgKeyword( 'createDate' ) ).toBeWithCase( 'createDate' );
      } );

      xit( "Expects getSourceTables to work", function() {
        transferService.getSourceTables( );
        var instanceVars = transferService.getInstanceVariables( );
        var expectedTables = [
          "accountantmodule",
          "accountanttext",
          "address",
          "answer",
          "answerquestionoption",
          "assignment",
          "category",
          "company",
          "contact",
          "content",
          "country",
          "customoption",
          "defaultoption",
          "dependency",
          "helptext",
          "language",
          "languagecountry",
          "logaction",
          "logentry",
          "logged",
          "module",
          "moduletext",
          "optionscore",
          "preferences",
          "question",
          "questionlist",
          "questionlisttext",
          "questionoption",
          "questiontext",
          "questiontype",
          "questiontypequestionoption",
          "report",
          "reportreason",
          "reportresult",
          "reporttext",
          "score",
          "securityrole",
          "securityroleitem",
          "specialistnote",
          "status",
          "tag",
          "tagmodule",
          "text",
          "textlocation",
          "usersession",
          "website",
          "websitetext"
        ];

        var foundSourceTables = structKeyArray( instanceVars.tables );

        for ( var table in expectedTables ) {
          expect( instanceVars.tables ).toHaveKey( table );
        }
      } );

      xit( "Expects initializeDestination / createDestinationTables to work", function() {
        transferService.getSourceTables( 'contact' );
        expect( function () { transferService.initializeDestination( ); } ).notToThrow( );
      } );

      xit( "Expects generateDestinationSQL to work", function() {
        transferService.getSourceTables( 'usersession' );
        transferService.initializeDestination( );
        transferService.generateDestinationSQL( );

        var instanceVars = transferService.getInstanceVariables();

        transferService.transferTable( instanceVars.items[ 1 ], 10 );
      } );
    } );
  }
}