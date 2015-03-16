tmsu-schemahandler
============================

tmsu-schemahandler implements a handler for the tmsu:// URI schema, on Unix/Linux OSes.
The system will automatically use it as needed once installed

This schema is designed mainly so that links to TMSU queries become possible. Personally, I particularly wanted
to be able to do this in Zim. Using the schema-handler machinery, not only is this possible, but it automatically works
in all programs which support linking to URIs.

examples:

* [tmsu://q/music+metal](tmsu://q/music+metal) or [tmsu://q/music metal](tmsu://q/music metal) -- play some metal.
* [tmsu://q/outgoing?in=doc&cmd=browser](tmsu://q/outgoing?in=doc&cmd=browser) -- show a list of all outgoing documents, in your browser.


Why not VFS?
-------------

If that's enough for your needs, there is no reason not to use TMSU's VFS mount system.
You can already link to a VFS mounted directory -- it will only open the directory in your file browser, though.

What I'm aiming for here is

a) more oneclick-ness, and
b) more configurability

than the VFS, or indeed TMSU itself.

To be able to simply click on a URI to edit a specific selection of files in GIMP, for example.
To sort and filter files in a more sophisticated way than TMSU (by tagcount, glob pattern, media characteristics), without constructing
strings of CLI incantations.


Installing
------------------

A script 'install.sh' is provided. Running it will install tmsu:// support for the user you are currently running as,
and support for tmsu:// URIs should become available within a few minutes after this.


Configuring
------------------

After installation, you will have a file ~/.config/tmsu-schema/config.sh, with some basic settings. You can then customize this,
by setting the WHERE or COMMAND variables, or modifying resolve_location() or invoke() to add the commands you want


Bugs
---------

There are no currently known bugs.

Querying for tag values in short form, eg tmsu://q/foo=bar , can look odd when
you also have options in the URI, though -- tmsu://q/foo=bar?in=doc&cmd=browser


Support
----------

Some systems explicitly strip links with unknown URI-schemas.
Currently known systems that do not support custom URI-schemas:

* Github markdown renderer (that's why the links in this document aren't clickable)


Plans
---------

* support other read-only functions such as `tags` and `imply`
    * generate nice HTML implication tables or graphs
* support full query options -- --directory, --file, --count, --path, --explicit, --sort etc.
* support further query options:
    * filter by tag count or glob pattern
    * More sorts:
      * By tag value or tag count
      * partial shuffle (preserving runs of N files in the same directory /same count/ same tag value..)
      * full shuffle (randomly reorder all items)
* Some reasonably secure way to pass extra command-specific options to invoke()
