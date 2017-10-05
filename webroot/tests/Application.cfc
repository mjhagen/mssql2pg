component {
  this.name = request.appName = "mssql2pg-tests";
  request.root = getDirectoryFromPath( getBaseTemplatePath( ) ) & "../../";
  this.mappings[ "/root" ] = request.root;
  this.mappings[ "/mustang" ] = expandPath( "../../../mustang-shared" );
  this.mappings[ "/testbox" ] = expandPath( "./testbox" );
  this.mappings[ "/tests" ] = expandPath( "./" );

  this.sessionManagement = true;
  this.datasources = deserializeJSON( fileRead( request.root & "/config/datasources.json" ) );

  public void function onRequestStart( ) {
    application.beanFactory = new framework.ioc(
      [
        "/root/model/services",
        "/mustang/services"
      ],
      {
        constants = {
          config = {
            showDebug = false,
            logLevel = "information",
            useOrm = false
          },
          root = request.root
        }
      }
    );
  }
}