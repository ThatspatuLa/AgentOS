#!/usr/bin/env python3
"""
Kanban API — Programmatic interface for Zen to create/update/move/delete tasks.

Usage:
    python3 kanban_api.py create --title "New task" --project kiyosaki --status backlog
    python3 kanban_api.py update task-kiyosaki-trading-system --field status --value wip
    python3 kanban_api.py move task-kiyosaki-trading-system --status validate
    python3 kanban_api.py delete task-kiyosaki-trading-system
    python3 kanban_api.py subtask-add task-kiyosaki-trading-system --title "New subtask"
    python3 kanban_api.py subtask-update task-kiyosaki-trading-system --subtask "M1" --field status --value done
    python3 kanban_api.py subtask-move task-kiyosaki-trading-system --subtask "M1" --status done
    python3 kanban_api.py list
    python3 kanban_api.py show task-kiyosaki-trading-system
    python3 kanban_api.py bump-version
"""

import json
import sys
import os
import argparse
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
TASKS_FILE = DATA_DIR / "kanban-tasks.json"

VALID_STATUSES = ["backlog", "wip", "validate", "done"]
VALID_PROJECTS = ["zen", "kiyosaki", "toji", "rin", "minato", "kazuki"]
VALID_PRIORITIES = ["Low", "Medium", "High"]
VALID_RISKS = ["Low", "Medium", "High"]


def load_tasks():
    if not TASKS_FILE.exists():
        return {"version": 1, "tasks": []}
    return json.loads(TASKS_FILE.read_text())


def save_tasks(data):
    DATA_DIR.mkdir(exist_ok=True)
    TASKS_FILE.write_text(json.dumps(data, indent=2))
    print(f"Saved {len(data['tasks'])} tasks (v{data['version']})")


def find_task(tasks, task_id):
    for t in tasks:
        if t["id"] == task_id:
            return t
    return None


def find_subtask(subtasks, title):
    for s in subtasks:
        if s["title"].lower().startswith(title.lower()):
            return s
    return None


def bump_version(data):
    data["version"] = data.get("version", 0) + 1
    return data


def cmd_create(args):
    data = load_tasks()
    tasks = data["tasks"]

    task_id = f"task-{args.id}" if args.id else f"task-{int(datetime.now().timestamp() * 1000)}"

    # Check for duplicate ID
    if find_task(tasks, task_id):
        print(f"ERROR: Task {task_id} already exists")
        sys.exit(1)

    task = {
        "id": task_id,
        "title": args.title,
        "project": args.project or "zen",
        "sessionId": None,
        "status": args.status or "backlog",
        "priority": args.priority or "Medium",
        "risk": args.risk or "Low",
        "created": datetime.now().strftime("%Y-%m-%d"),
        "lastActive": "Now",
        "archived": False,
        "why": args.why or "",
        "nextAction": args.next_action or "",
        "notes": "",
        "subtasks": [],
        "evidence": [],
        "validation": [],
        "activity": [f"Created by Zen via kanban_api.py"],
    }

    tasks.insert(0, task)
    bump_version(data)
    save_tasks(data)
    print(f"Created: {task_id} — {args.title}")


def cmd_update(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    field = args.field
    value = args.value

    # Handle special fields
    if field == "status" and value not in VALID_STATUSES:
        print(f"ERROR: Invalid status '{value}'. Valid: {VALID_STATUSES}")
        sys.exit(1)
    if field == "project" and value not in VALID_PROJECTS:
        print(f"ERROR: Invalid project '{value}'. Valid: {VALID_PROJECTS}")
        sys.exit(1)

    # Convert types
    if value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
    elif value.lower() == "null" or value.lower() == "none":
        value = None

    old_val = task.get(field)
    task[field] = value
    task["lastActive"] = "Now"
    task.setdefault("activity", []).append(f"Updated {field}: {old_val} → {value}")

    bump_version(data)
    save_tasks(data)
    print(f"Updated {args.task_id}: {field} = {value}")


def cmd_move(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    if args.status not in VALID_STATUSES:
        print(f"ERROR: Invalid status '{args.status}'. Valid: {VALID_STATUSES}")
        sys.exit(1)

    old_status = task["status"]
    task["status"] = args.status
    task["lastActive"] = "Now"
    task.setdefault("activity", []).append(f"Moved: {old_status} → {args.status}")

    bump_version(data)
    save_tasks(data)
    print(f"Moved {args.task_id}: {old_status} → {args.status}")


def cmd_delete(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    if not args.force:
        confirm = input(f"Delete '{task['title']}' ({args.task_id})? [y/N]: ")
        if confirm.lower() != "y":
            print("Aborted")
            sys.exit(0)

    tasks.remove(task)
    bump_version(data)
    save_tasks(data)
    print(f"Deleted: {args.task_id}")


def cmd_archive(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    task["archived"] = True
    task["lastActive"] = "Now"
    task.setdefault("activity", []).append("Archived")

    bump_version(data)
    save_tasks(data)
    print(f"Archived: {args.task_id}")


def cmd_subtask_add(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    subtasks = task.setdefault("subtasks", [])

    # Check for duplicate
    if find_subtask(subtasks, args.title):
        print(f"ERROR: Subtask '{args.title}' already exists")
        sys.exit(1)

    subtask = {
        "title": args.title,
        "status": args.status or "backlog",
        "description": args.description or "",
        "notes": "",
        "nextAction": "",
        "evidence": [],
        "activity": [],
    }

    subtasks.append(subtask)
    task["lastActive"] = "Now"
    task.setdefault("activity", []).append(f"Added subtask: {args.title}")

    bump_version(data)
    save_tasks(data)
    print(f"Added subtask '{args.title}' to {args.task_id}")


def cmd_subtask_update(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    subtask = find_subtask(task.get("subtasks", []), args.subtask)
    if not subtask:
        print(f"ERROR: Subtask '{args.subtask}' not found in {args.task_id}")
        sys.exit(1)

    field = args.field
    value = args.value

    if field == "status" and value not in VALID_STATUSES:
        print(f"ERROR: Invalid status '{value}'. Valid: {VALID_STATUSES}")
        sys.exit(1)

    old_val = subtask.get(field)
    subtask[field] = value
    task["lastActive"] = "Now"
    task.setdefault("activity", []).append(f"Updated subtask '{subtask['title']}': {field} = {value}")

    bump_version(data)
    save_tasks(data)
    print(f"Updated subtask '{subtask['title']}': {field} = {value}")


def cmd_subtask_move(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    subtask = find_subtask(task.get("subtasks", []), args.subtask)
    if not subtask:
        print(f"ERROR: Subtask '{args.subtask}' not found in {args.task_id}")
        sys.exit(1)

    if args.status not in VALID_STATUSES:
        print(f"ERROR: Invalid status '{args.status}'. Valid: {VALID_STATUSES}")
        sys.exit(1)

    old_status = subtask["status"]
    subtask["status"] = args.status
    task["lastActive"] = "Now"
    task.setdefault("activity", []).append(f"Moved subtask '{subtask['title']}': {old_status} → {args.status}")

    bump_version(data)
    save_tasks(data)
    print(f"Moved subtask '{subtask['title']}': {old_status} → {args.status}")


def cmd_list(args):
    data = load_tasks()
    tasks = data["tasks"]

    project_filter = args.project if hasattr(args, "project") and args.project else None
    status_filter = args.status if hasattr(args, "status") and args.status else None

    filtered = tasks
    if project_filter:
        filtered = [t for t in filtered if t["project"] == project_filter]
    if status_filter:
        filtered = [t for t in filtered if t["status"] == status_filter]

    print(f"Kanban Tasks (v{data['version']}) — {len(filtered)} of {len(tasks)} shown\n")

    # Group by status
    for status in VALID_STATUSES:
        status_tasks = [t for t in filtered if t["status"] == status]
        if status_tasks:
            print(f"  [{status.upper()}] ({len(status_tasks)})")
            for t in status_tasks:
                archived = " [ARCHIVED]" if t.get("archived") else ""
                subtask_count = len(t.get("subtasks", []))
                done_count = sum(1 for s in t.get("subtasks", []) if s.get("status") == "done")
                print(f"    {t['id']}: {t['title']}{archived} ({done_count}/{subtask_count} subtasks)")
            print()


def cmd_show(args):
    data = load_tasks()
    tasks = data["tasks"]
    task = find_task(tasks, args.task_id)

    if not task:
        print(f"ERROR: Task {args.task_id} not found")
        sys.exit(1)

    print(f"{'='*60}")
    print(f"Task: {task['title']}")
    print(f"ID: {task['id']}")
    print(f"Project: {task['project']} | Status: {task['status']} | Priority: {task['priority']} | Risk: {task['risk']}")
    print(f"Created: {task['created']} | Last Active: {task['lastActive']}")
    print(f"\nWhy: {task.get('why', 'N/A')}")
    print(f"Next Action: {task.get('nextAction', 'N/A')}")
    print(f"Notes: {task.get('notes', 'N/A')}")

    subtasks = task.get("subtasks", [])
    if subtasks:
        print(f"\nSubtasks ({len(subtasks)}):")
        for s in subtasks:
            print(f"  [{s['status']}] {s['title']}")
            if s.get("description"):
                print(f"         {s['description'][:80]}")

    evidence = task.get("evidence", [])
    if evidence:
        print(f"\nEvidence: {', '.join(evidence)}")

    validation = task.get("validation", [])
    if validation:
        print(f"Validation: {', '.join(validation)}")

    activity = task.get("activity", [])
    if activity:
        print(f"\nActivity ({len(activity)} entries):")
        for a in activity[-5:]:
            print(f"  - {a}")

    print(f"{'='*60}")


def cmd_bump_version(args):
    data = load_tasks()
    old = data.get("version", 0)
    bump_version(data)
    save_tasks(data)
    print(f"Version: {old} → {data['version']}")


def main():
    parser = argparse.ArgumentParser(description="Kanban API — Zen's task management interface")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # create
    p = subparsers.add_parser("create", help="Create a new task")
    p.add_argument("--title", required=True)
    p.add_argument("--project", default="zen")
    p.add_argument("--status", default="backlog")
    p.add_argument("--priority", default="Medium")
    p.add_argument("--risk", default="Low")
    p.add_argument("--why", default="")
    p.add_argument("--next-action", default="")
    p.add_argument("--id", default=None)

    # update
    p = subparsers.add_parser("update", help="Update a task field")
    p.add_argument("task_id")
    p.add_argument("--field", required=True)
    p.add_argument("--value", required=True)

    # move
    p = subparsers.add_parser("move", help="Move task to a different status column")
    p.add_argument("task_id")
    p.add_argument("--status", required=True)

    # delete
    p = subparsers.add_parser("delete", help="Delete a task")
    p.add_argument("task_id")
    p.add_argument("--force", action="store_true")

    # archive
    p = subparsers.add_parser("archive", help="Archive a task")
    p.add_argument("task_id")

    # subtask-add
    p = subparsers.add_parser("subtask-add", help="Add a subtask")
    p.add_argument("task_id")
    p.add_argument("--title", required=True)
    p.add_argument("--status", default="backlog")
    p.add_argument("--description", default="")

    # subtask-update
    p = subparsers.add_parser("subtask-update", help="Update a subtask field")
    p.add_argument("task_id")
    p.add_argument("--subtask", required=True, help="Subtask title (prefix match)")
    p.add_argument("--field", required=True)
    p.add_argument("--value", required=True)

    # subtask-move
    p = subparsers.add_parser("subtask-move", help="Move subtask to different status")
    p.add_argument("task_id")
    p.add_argument("--subtask", required=True)
    p.add_argument("--status", required=True)

    # list
    p = subparsers.add_parser("list", help="List all tasks")
    p.add_argument("--project", default=None)
    p.add_argument("--status", default=None)

    # show
    p = subparsers.add_parser("show", help="Show task details")
    p.add_argument("task_id")

    # bump-version
    p = subparsers.add_parser("bump-version", help="Bump the file version")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cmds = {
        "create": cmd_create,
        "update": cmd_update,
        "move": cmd_move,
        "delete": cmd_delete,
        "archive": cmd_archive,
        "subtask-add": cmd_subtask_add,
        "subtask-update": cmd_subtask_update,
        "subtask-move": cmd_subtask_move,
        "list": cmd_list,
        "show": cmd_show,
        "bump-version": cmd_bump_version,
    }

    cmds[args.command](args)


if __name__ == "__main__":
    main()