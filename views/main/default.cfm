<div class="jumbotron">
  <h1 class="display-4">
    Transfer Microsoft SQL&nbsp;Server to PostgreSQL
  </h1>

  <form>
    <button id="import" type="button" class="btn btn-primary">Go now, go!</button>
  </form>
</div>

<div id="import-progress" class="hidden">
  <div class="progress">
    <div class="progress-bar progress-bar-striped active" style="width:0%;"></div>
  </div>
</div>

<div id="import-status" class="well well-sm">
  <cfoutput>#i18n.translate( 'click-start-import-instruction' )#</cfoutput>
</div>

<div id="import-log" class="well well-sm hidden"></div>