component accessors=true {
  property framework;
  property transferService;

  function run( rc ) {
    cfsetting( requesttimeout = 300 );

    param rc.runSingleThreaded=false;
    param rc.skipTransfer=false;
    transferService.start( rc.runSingleThreaded, rc.skipTransfer );

    var message = "Transfer started";

    if ( rc.runSingleThreaded ) {
      message = "Transfer done";
    }

    if ( rc.skipTransfer ) {
      message &= " (skipping transfer)";
    }

    framework.renderData( "text", message );
  }

  function getSyncProgress( rc ) {
    var data = transferService.getSyncProgress( );
    framework.renderData( "json", data );
  }

  function abortQueue( rc ) {
    transferService.abortQueue( );
    framework.renderData( "json", true );
  }
}