component extends="framework.one" {
  request.appName = "mssql2pg";
  request.version = "1.0";
  request.root = getRoot( );

  this.mappings[ "/root" ] = request.root;
  this.mappings[ "/mustang" ] = expandPath( "../../mustang-shared" );

  this.sessionManagement = true;
  this.datasources = deserializeJSON( fileRead( request.root & "/config/datasources.json" ) );

  variables.framework = {
    base = "/root",
    diLocations = [
      "/root/model/services",
      "/mustang/services"
    ],
    diConfig = {
      constants = {
        root = request.root,
        config = {
          showDebug = true,
          logLevel = "debug",
          useOrm = false
        }
      }
    }
  };

  public void function setupRequest( ) {
    request.reset = isFrameworkReloadRequest( );
    variables.i18n = getBeanFactory( ).getBean( "translationService" );
  }

  private string function getRoot( string basePath = getDirectoryFromPath( getBaseTemplatePath( ) ) ) {
    var tmp = replace( basePath, "\", "/", "all" );
    return listDeleteAt( tmp, listLen( tmp, "/" ), "/" ) & "/";
  }
}