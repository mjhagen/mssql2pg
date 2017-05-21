component extends = "testbox.system.BaseSpec" {
  lock scope = "application" timeout = "3" {
    variables.beanFactory = application.beanFactory;
  }

  public void function beforeAll() {
    addMatchers({
               toBeJSON = function( expectation, args = {}) { return isJSON( expectation.actual ); },
            notToBeJSON = function( expectation, args = {}) { return !isJSON( expectation.actual ); },
         toBeInstanceOf = function( expectation, args = {}) { return isInstanceOf( expectation.actual, args[1] ); },
      notToBeInstanceOf = function( expectation, args = {}) { return !isInstanceOf( expectation.actual, args[1] ); },
         toHaveKeyValue = function( expectation, args = {}) { return arrayLen( structFindValue( { str = expectation.actual }, args[1], "one" )) > 0; }
    });
  }
}