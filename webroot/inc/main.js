var $importProgress, $importStatus, $importBtn, $abortBtn, prevItem, progressUpdate;
var defaultInterval = 150;
var intervalIncrease = 75;
var intervalTime = defaultInterval;

$( function( ) {
  $importProgress = $( '#import-progress' );
  $importStatus = $( '#import-status' );
  $importBtn = $( '#import' );
  $abortBtn = $( '#abort' );
  $abortBtn.hide( );

  checkProgress( );

  $( document ).on( 'click', '#import', function( ) {
    progressUpdate = setTimeout( checkProgress, intervalTime );

    $.ajax( ajaxUrl( 'adminapi:transfer', 'run' ), {
      complete : showProgress
    } );
  } );

  $( document ).on( 'click', '#abort', function( ) {
    $.ajax( ajaxUrl( 'adminapi:transfer', 'abortQueue' ), {
      complete : function( ) {
        $importStatus.html( '' );
        hideProgress( );
        clearTimeout( progressUpdate );
      }
    } );
  } );
} );

function checkProgress( ) {
  $.ajax( ajaxUrl( 'adminapi:transfer', 'getSyncProgress' ), {
    success : function( data ) {
      if( data.hasOwnProperty( 'done' ) && data.done ) {
        $importStatus.html( data.status );
        hideProgress( );
      } else {
        showProgress( );

        if( data.current == 0 ) {
          $importStatus.html( 'Calculating total number of tables: ' + data.total + '<br />' + data.status );
        } else {
          var newPercentage = 0;

          if( data.total > 0 ) {
            newPercentage = ( 100 / parseInt( data.total ) * parseInt( data.current ));
          }

          newPercentage = ( Math.round( newPercentage * 100 ) / 100 ) + '%';

          $importProgress.find( '.progress-bar' ).css({ width : newPercentage } );
          $importStatus.html( data.timeLeft + ' <span class="text-muted">(' + data.current + '/' + data.total + ')</span><br />' + data.status );
        }

        // progress complete:
        if( data.current == prevItem ) {
          intervalTime += intervalIncrease;
        } else {
          intervalTime = defaultInterval;
        }

        progressUpdate = setTimeout( checkProgress, intervalTime );
        prevItem = data.current;
      }
    },
    error : function( ) {
      $importStatus.html( 'Error occured, contact E-Line Websolutions' );
    }
  } );
}

function hideProgress( ) {
  $importProgress.find( '.progress-bar' ).css({ width : '0%' } );
  $importBtn.removeProp( 'disabled' );
  $abortBtn.hide( );
}

function showProgress( ) {
  $importProgress.removeClass( 'hidden' );
  $importBtn.prop( 'disabled', true );
  $abortBtn.show( );
}

function ajaxUrl( action, method, data ) {
  var attributes = "";

  if ( data && data.length ) {
    for ( var i = 0; i < data.length; i++ ) {
      attributes += data[i][0] + "/" + data[i][1] + "/";
    }
  }

  return _webroot + "/" + action + "/" + method + "/" + attributes;
}
