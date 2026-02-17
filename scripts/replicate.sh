#!/bin/bash

# Create tmux sessions
tmux new-session -d -s t1
tmux new-session -d -s t2
tmux new-session -d -s t3

# Define the sequence of commands to execute across sessions
sequence=(
  "t1; echo 't1 command 1'"
  "t2; echo 't2 command 1'"
  "t1; echo 't1 command 2'"
  "t2; echo 't2 command 2'"
  "t1; echo 't1 command 3'"
  "t3; echo 't3 command 1'"
  "t3; echo 't3 command 2'"
  "t1; echo 't1 command 4'"
)

# Iterate through the sequence and execute commands
for item in "${sequence[@]}"; do
  session=${item%%;*}
  command=${item#*;}
  tmux send-keys -t "$session" "$command" Enter
  sleep 1 # Pause briefly to observe the sequence
done

echo "Sequence completed."

