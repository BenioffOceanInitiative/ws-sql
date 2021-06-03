# ws-sql
WhaleSafe standard query language processing with BigQuery

## html

These web pages (\*.html) are typically rendered from Rmarkdown (\*.Rmd):

<!-- Jekyll rendering: -->
{% for file in site.static_files %}
  {% if file.extname == '.html' %}
* [{{ file.basename }}]({{ site.baseurl }}{{ file.path }})
  {% endif %}
{% endfor %}
