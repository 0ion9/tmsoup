#!/bin/sh
#
# Install tmsu schema handler in the user's home directory.
#
# You can run this as root, but it will install it for root, NOT site-wide.
#

install -vm 0755 tmsu-schemahandler.sh ~/bin/

sed s,HOME/,$HOME/,g < tmsu-schemahandler.desktop > /tmp/tmsu-schemahandler.desktop

install -vm 0644 /tmp/tmsu-schemahandler.desktop ~/.local/share/applications/

if [ ! -f ~/.config/tmsu-schema/config.sh ]; then
  install -vm 0644 config.sh ~/.config/tmsu-schema/
fi

rm /tmp/tmsu-schemahandler.desktop
