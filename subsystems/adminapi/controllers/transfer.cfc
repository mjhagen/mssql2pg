component accessors=true {
  property framework;
  property transferService;

  function run( rc ) {
    transferService.start( );
    framework.renderData( "text", "Transfer started" );
  }

  function getSyncProgress( rc ) {
    var data = transferService.getSyncProgress( );
    framework.renderData( "json", data );
  }
}