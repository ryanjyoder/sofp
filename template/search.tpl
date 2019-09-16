<html>
<head>
</head>
<body>

  <form id="search" action="/search" method="get" class="searchbar js-searchbar " autocomplete="off" role="search">
                    <div class="ps-relative">
                        <input name="q" type="text" placeholder="Search…" autocomplete="off" maxlength="240" class="s-input js-search-field "  value="{{.Query}}">
                        <button type="submit" aria-label="Search…" class="s-btn s-btn__primary s-btn__icon btn-topbar-primary js-search-submit"><svg aria-hidden="true" class="svg-icon iconSearch" width="18" height="18" viewBox="0 0 18 18"><path d="M12.86 11.32L18 16.5 16.5 18l-5.18-5.14v-.35a7 7 0 1 1 1.19-1.19h.35zM7 12A5 5 0 1 0 7 2a5 5 0 0 0 0 10z"></path></svg></button>
                    </div>
            </form>


<h2>Results</h2>
{{ range .Results }}
    <p>
        <a href="{{.Href}}" title="{{.Title}}" >{{.Title}}</a> <br />
        {{.Summary}}
    </p>
{{ end }}

</body>
</html>