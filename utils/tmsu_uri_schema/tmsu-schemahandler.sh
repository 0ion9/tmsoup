#!/bin/sh

# the config file must define two functions:
# * resolve_location
#   * Output a directory given $1 == directory id key (eg 'pic'). Return '' for unknown directory id keys.
# * invoke
#   * Accept a filelist from stdin and feed it into a program identified by a key $1, storing output into the file named by $2.
#     Reject unknown commands (show an error notification)
#   * $3 is a description of the query, including (options).Further arguments after $3 may be passed. You can use this to provide some customization of the launch, but
#     it's exact meaning is currently undefined.
#
# and may specify two variables:
# * WHERE specifies a default directory id key to pass to resolve_location() if no key is explicitly specified
#         (used by /q/)
# * COMMAND specifies a default command id key to pass to invoke()
#         (used by /q/)
#

ME="TMSU schema-handler"

. ~/.config/tmsu-schema/config.sh

COMMAND=${COMMAND:-xdg}
notify-send "where: " "$WHERE"
WHERE=${WHERE:-doc}

notify-send "where: " "$WHERE"

URI="$1"
STRIPPED=${URI##tmsu://}
#$(echo "$URI" |sed -Ee 's,^tmsu://,,')
notify-send "$ME got stripped:" "$STRIPPED"

ACTION=${STRIPPED%%/*}
notify-send "$ME got action:" "$ACTION"

ARGS=${STRIPPED#*/}
notify-send "$ME got args:" "$ARGS"
mkdir -p /tmp/tmsu-sch/$ACTION

OPTIONS=${ARGS##*\?}
if [ "$OPTIONS" == "$ARGS" ]; then
  OPTIONS=
else
  notify-send "$ME got options:" "$OPTIONS"
fi

if [ -n "$OPTIONS" ]; then
  ARGS=${ARGS%%\?*}
fi

case "$ACTION" in
 "q")
    # XXX also handle ?foo to set how we want to use the results, etc
    #
    # default location to query from is ~/xm
    FINALARGS=$(echo "$ARGS" | sed -e 's/+/ /g')
    OUTFILE=/tmp/tmsu-sch/q/"$ARGS".$PPID
    if [ -n "$OPTIONS" ]; then
        IFS="&"
        for PAIR in $OPTIONS; do
            LHS=${PAIR%%=*}
            RHS=${PAIR##*=}
            notify-send "$ME option-parsing" "LHS = $LHS; RHS = $RHS"
            case "$LHS" in
                "in") WHERE="$RHS";;
                "cmd") COMMAND="$RHS";;
                *) notify-send "$ME option-parsing" "Unknown parameter $LHS=$RHS for /q/";;
            esac
        done
        IFS=
    fi

    WHERE=$(resolve_location "$WHERE")
    if [ -z "$WHERE" ]; then
        notify-send "$ME location key" "Location key $WHERE unknown, exiting."
        exit 1
    fi

    notify-send "$ME ($PPID) : action =q" "Finalargs = $FINALARGS"

    cd "$WHERE"
    if [ -n "$OPTIONS" ]; then
        QUERYINFO="$FINALARGS ($OPTIONS)"
    else
        QUERYINFO="$FINALARGS"
    fi
    tmsu files "$FINALARGS" | invoke "$COMMAND" "$OUTFILE" "$QUERYINFO"&
    ;;
 *) notify-send "$ME" "got unknown action : $ACTION"
    exit 1;;
esac
