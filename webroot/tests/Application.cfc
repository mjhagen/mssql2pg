component {
  this.sessionManagement = true;
  this.mappings[ "/root" ] = getDirectoryFromPath( getBaseTemplatePath( ) ) & "../../";
  this.mappings[ "/mustang" ] = expandPath( "../../../mustang-shared" );
  this.mappings[ "/testbox" ] = expandPath( "./testbox" );
  this.mappings[ "/tests" ] = expandPath( "./" );

  public void function onRequestStart( ) {
    application.beanFactory = new framework.ioc(
      [
        "/root/model/services",
        "/mustang/services"
      ],
      {
        constants = {
          root = "/root"
        }
      }
    );
  }
}