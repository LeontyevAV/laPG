import subprocess
import os
import sys

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = os.path.join(PROJECT_DIR, "venv", "bin", "python")
LOG_FILE = os.path.join(PROJECT_DIR, "backup", "cron.log")


def get_crontab():
    try:
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, check=True
        )
        return result.stdout
    except subprocess.CalledProcessError:
        return ""


def set_crontab(content):
    try:
        subprocess.run(
            ["crontab", "-"], input=content, text=True, capture_output=True, check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def restart_cron():
    for cmd in (
        ["sudo", "service", "cron", "restart"],
        ["sudo", "systemctl", "restart", "cron"],
    ):
        try:
            subprocess.run(cmd, capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    return False


def parse_lines(content):
    return content.splitlines(keepends=True)


def build_backup_command(backup_all=True, keep=0):
    python = VENV_PYTHON if os.path.exists(VENV_PYTHON) else "python3"
    parts = [python, os.path.join(PROJECT_DIR, "backup.py")]
    if backup_all:
        parts.append("--all")
    if keep:
        parts.extend(["--keep", str(keep)])
    return " ".join(parts)


def build_cron_line(schedule, command, tag="laPG"):
    cmd = f"cd {PROJECT_DIR} && {command} >> {LOG_FILE} 2>&1"
    return f"{schedule} {cmd}  # {tag}"


def list_project_entries(lines):
    result = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "laPG" in stripped:
            result.append((i, stripped.rstrip()))
    return result


def prompt_schedule():
    print("\nРасписание:")
    print("1. Каждый час")
    print("2. Каждый день")
    print("3. Каждую неделю")
    print("4. Своё выражение")
    sc = input("Выберите: ").strip()
    if sc == "1":
        return "0 * * * *"
    elif sc == "2":
        h = input("Час (0-23): ").strip()
        m = input("Минута (0-59): ").strip()
        h = h if h.isdigit() and 0 <= int(h) <= 23 else "2"
        m = m if m.isdigit() and 0 <= int(m) <= 59 else "0"
        return f"{m} {h} * * *"
    elif sc == "3":
        d = input("День недели (0-7, где 0 и 7 = вс): ").strip()
        d = d if d.isdigit() and 0 <= int(d) <= 7 else "0"
        h = input("Час (0-23): ").strip()
        h = h if h.isdigit() and 0 <= int(h) <= 23 else "3"
        return f"0 {h} * * {d}"
    elif sc == "4":
        return input("Введите cron-выражение (5 полей): ").strip()
    return None


def cron_manager():
    while True:
        content = get_crontab()
        lines = parse_lines(content)
        entries = list_project_entries(lines)

        print("\n--- Настройка cron ---")
        if entries:
            print("Текущие задачи laPG:")
            for idx, (_, entry) in enumerate(entries, 1):
                print(f"  {idx}. {entry}")
        else:
            print("Нет задач laPG в cron.")

        print("\n1. Добавить задачу")
        print("2. Удалить задачу")
        if entries:
            print("3. Редактировать задачу")
            print("0. Назад")
        else:
            print("0. Назад")

        choice = input("\nВыберите: ").strip()

        if choice == "0":
            break

        elif choice == "1":
            schedule = prompt_schedule()
            if schedule is None:
                print("Неверный выбор.")
                continue

            print("\nКоманда:")
            print("1. Бекап всех БД")
            print("2. Бекап всех БД с ротацией")
            print("3. Своя команда")
            cc = input("Выберите: ").strip()
            if cc == "1":
                command = build_backup_command(backup_all=True)
            elif cc == "2":
                k = input("Сколько бекапов хранить? (по умолчанию 10): ").strip()
                keep = int(k) if k.isdigit() else 10
                command = build_backup_command(backup_all=True, keep=keep)
            elif cc == "3":
                command = input("Введите команду: ").strip()
            else:
                print("Неверный выбор.")
                continue

            cron_line = build_cron_line(schedule, command)
            lines.append(cron_line + "\n")
            if set_crontab("".join(lines)):
                print("Задача добавлена.")
                if restart_cron():
                    print("Cron перезапущен.")
                else:
                    print("Не удалось перезапустить cron. Сделайте это вручную: sudo service cron restart")
            else:
                print("Ошибка при сохранении crontab.")
                continue

        elif choice in ("2", "3") and entries:
            if len(entries) == 1:
                idx = 0
            else:
                try:
                    idx = int(input("Введите номер: ").strip()) - 1
                    if idx < 0 or idx >= len(entries):
                        print("Неверный номер.")
                        continue
                except ValueError:
                    print("Введите число.")
                    continue

            entry_index, _ = entries[idx]

            if choice == "2":
                confirm = input(f"Удалить задачу {idx + 1}? (yes/no): ").strip().lower()
                if confirm not in ("yes", "y"):
                    continue
                lines.pop(entry_index)

            elif choice == "3":
                confirm = input(f"Редактировать задачу {idx + 1}? (yes/no): ").strip().lower()
                if confirm not in ("yes", "y"):
                    continue
                lines.pop(entry_index)

                schedule = prompt_schedule()
                if schedule is None:
                    print("Неверный выбор.")
                    continue

                print("\nНовая команда:")
                print("1. Бекап всех БД")
                print("2. Бекап всех БД с ротацией")
                print("3. Своя команда")
                cc = input("Выберите: ").strip()
                if cc == "1":
                    command = build_backup_command(backup_all=True)
                elif cc == "2":
                    k = input("Сколько бекапов хранить? (по умолчанию 10): ").strip()
                    keep = int(k) if k.isdigit() else 10
                    command = build_backup_command(backup_all=True, keep=keep)
                elif cc == "3":
                    command = input("Введите команду: ").strip()
                else:
                    print("Неверный выбор.")
                    continue

                cron_line = build_cron_line(schedule, command)
                lines.append(cron_line + "\n")

            if set_crontab("".join(lines)):
                print("Изменения сохранены.")
                if restart_cron():
                    print("Cron перезапущен.")
                else:
                    print("Не удалось перезапустить cron. Сделайте вручную: sudo service cron restart")
            else:
                print("Ошибка при сохранении crontab.")

        else:
            print("Неверный пункт.")
