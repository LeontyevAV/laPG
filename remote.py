import subprocess
from settings import get_settings, save_remote_config

_, _, _, YAML_CFG = get_settings()


def show_top_processes(lines=20):
    rc = YAML_CFG.remote

    default_host = rc.host or ""
    default_user = rc.user or "root"

    prompt_host = f"  Хост [{default_host}]: " if default_host else "  Хост: "
    host = input(prompt_host).strip() or default_host
    if not host:
        print("  Хост не указан.")
        return
    ssh_port = input(f"  SSH порт [{rc.port}]: ").strip() or str(rc.port)
    user = input(f"  Пользователь [{default_user}]: ").strip() or default_user

    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=accept-new",
        "-p", ssh_port,
        f"{user}@{host}",
        f"ps aux --sort=-%cpu | head -{lines}",
    ]

    print(f"\nПодключение к {user}@{host}:{ssh_port}...")
    try:
        result = subprocess.run(cmd, timeout=15)
        if result.returncode == 0:
            save_remote_config(host, user, int(ssh_port), rc.pg_port)
    except FileNotFoundError:
        print("  Ошибка: ssh не найден")
    except subprocess.TimeoutExpired:
        print("  Ошибка: таймаут подключения")


if __name__ == "__main__":
    show_top_processes()
