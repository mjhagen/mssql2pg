component {
  this.mappings[ "/root" ] = getDirectoryFromPath( getBaseTemplatePath( ) ) & "../../";
  this.mappings[ "/mustang" ] = expandPath( "../../../mustang-shared" );
  this.mappings[ "/testbox" ] = expandPath( "./testbox" );
  this.mappings[ "/tests" ] = expandPath( "./" );

  this.sessionManagement = true;
  this.datasources = deserializeJSON( fileRead( expandPath( "/root/config/test-datasources.json" ) ) );

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
          root = "/root"
        }
      }
    );
  }
}