# >>> Load user‑defined aliases if the file exists <<<
if [ -f "$HOME/.bash_aliases" ]; then
    . "$HOME/.bash_aliases"
fi
# >>> End of alias loading <<<

# ---------------------------------
#   USER DEFINED BASH ALIASES
# ---------------------------------


# -------------------------------------------------
#  1️⃣  Development
# -------------------------------------------------
alias vue-v='npm list vuetify'            # list vuetify version
alias dev-api='uvicorn main:app --reload' # start up fast api in dev
alias pyenv-v='pyenv virtualenvs'         # list available virtual environments
alias sa='source activate'                # activate virtual environments
alias sv='source .venv/bin/activate'
# -------------------------------------------------
#  1️⃣  Navigation & Directory Management
# -------------------------------------------------
alias ..='cd ..'                                                    # go up one level
alias ...='cd ../..'                                                # go up two levels
alias ....='cd ../../..'                                            # go up three levels
alias ~='cd ~'                                                      # shortcut for home (works in most shells)
alias c='clear'                                                     # clear the screen
alias md='mkdir -p'                                                 # create a directory tree without error if it exists
alias rd='rmdir'                                                    # remove empty directories
alias rdp='rmdir -p'                                                # removes a directory *and* its empty parents
cdf() {
    # If no argument was supplied, show usage and return
    if [[ -z $1 ]]; then
        echo "Usage: cdf <pattern>"
        return 1
    fi

    # Find the first directory that matches the pattern (case‑sensitive)
    target=$(find . -type d -name "*$1*" -print -quit)

    # If find didn't locate anything, tell the user
    if [[ -z $target ]]; then
        echo "No directory matching '*$1*' found."
        return 1
    fi

    # Change into the directory
    cd "$target" || return  # cd will fail only on race conditions
}
    # quick “cd into first matching dir” – use `cdf pattern`


# -------------------------------------------------
#  2️⃣  Listing / File Inspection
# -------------------------------------------------
alias ll='ls -alFh --color=auto'         # long view, show hidden, type indicators, human‑readable sizes
alias la='ls -A --color=auto'            # all except . and ..
alias l='ls -CF --color=auto'            # columns, classify
alias lt='ls -1t --color=auto'           # newest first, one per line
alias lsd='ls -d */'                     # list only directories
alias tree='tre' \
    # colour‑tree, show hidden files, ignore noise

# -------------------------------------------------
#  3️⃣  File Operations
# -------------------------------------------------
alias cpv='cp -iv'                        # copy with interactive prompt & verbose
alias mvv='mv -iv'                        # move with interactive prompt & verbose
alias rmf='rm -rf'                        # force‑remove (use with caution!)
alias mkcd='foo(){ mkdir -p "$1" && cd "$1"; }; foo' 
    # create a directory and immediately cd into it


showvar() {
    local var_name=$1
    # Show a helpful message when the variable is missing
    if [[ -z ${!var_name+x} ]]; then
        printf 'Variable %s is not set\n' "$var_name" >&2
        return 1
    fi
    printf '%s\n' "${!var_name}"
}

# Create a file
newfile() {
    # If no argument given, fall back to a timestamped name
    if [[ -z $1 ]]; then
        name="$(date +%Y-%m-%d_%H-%M-%S).txt"
    else
        name="$1"
    fi

    # Prevent overwriting an existing file unless you explicitly ask
    if [[ -e $name ]]; then
        read -p "File \"$name\" already exists. Overwrite? (y/N) " ans
        [[ $ans != [Yy] ]] && echo "Aborted." && return 1
    fi

    touch "$name"
    echo "Created \"$name\" in $(pwd)"
}
# Optional: expose a short alias that just forwards to the function
alias nf='newfile'



    echo "✅ Done! $tag_name now includes the new commit."
}

alias release_ic='release_into_current'
