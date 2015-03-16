#!/bin/sh

resolve_location()
{
 case "$1" in
   pic)
      echo ~/Pictures;;
   doc)
      echo ~/Documents;;
   mus)
      echo ~/Music;;
   *) echo '';;
 esac
}

invoke()
{
 COMMAND="$1"
 OUTFILE="$2"
 shift 2
 case "$COMMAND" in
   xdg)
       tr \\n \\0 | xargs -0 xdg-open

   sxiv)
       sxiv -i -o "$@" > "$OUTFILE";;

   feh)
       # feh saves the edited filelist over the original.
       # obviously it can't do this if we give the filelist over stdin.
       cat > "$OUTFILE"
       feh --filelist "$OUTFILE";;

   aud)
       tr \\n \\0 | xargs -0 audacious -E;;

   mpv)
       tr \\n \\0 | xargs -0 mpv;;
   browser)
   #simple unstyled html links
       HTMLFILE="${OUTFILE}.html"
       python -c 'import sys
import os
try:
    # py3
    from urllib.parse import quote
except ImportError:
    # py2
    from urllib import quote
sys.stdout.write("<html><body>")
items = [os.path.abspath(p) for p in sys.stdin.read().splitlines()]
commonprefix = os.path.commonprefix([os.path.dirname(p) for p in items])
if not os.path.isdir(commonprefix):
    # /foo/bar/partial-dirnam stupidity detected
    commonprefix = os.path.dirname(commonprefix)
start = len(commonprefix)
sys.stdout.write("<h2>%d items in query %r:</h2><p><ul>\n" % (len(items), sys.argv[1]))
for p in items:
    esc = quote(p, safe="/")
    sys.stdout.write("<li><a href=\"file://%s\">%s</a></li>\n" % (esc, p[start:].lstrip(os.path.sep)))
sys.stdout.write("</ul></body></html>")

' "$@" > "$HTMLFILE"
       xdg-open "$HTMLFILE";;

   *)
       notify-send "$ME: Error" "$COMMAND is not an approved command."
 esac
}

COMMAND=xdg
WHERE=doc