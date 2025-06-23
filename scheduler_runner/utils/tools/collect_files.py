from pathlib import Path

def main():
    # Список относительных путей к файлам, которые нужно включить
    files_to_include = [
        # scheduler_runner\utils:
        "../logging.py",
        "../subprocess.py",
        "../timing.py",
        # scheduler_runner:
        "../../runner.py",
        "../../schedule_config.py",
    ]

    # Путь к выходному файлу (в том же каталоге, что и скрипт)
    output_file = Path("PythonCode.txt")

    # Получить абсолютный путь к директории скрипта
    script_dir = Path(__file__).parent

    # Предполагаем, что корневая директория проекта на два уровня выше
    root_dir = script_dir.parent.parent.parent

    # Открываем выходной файл для записи
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for rel_path in files_to_include:
            # Разрешаем относительный путь к абсолютному
            file_path = (script_dir / rel_path).resolve()
            try:
                # Вычисляем относительный путь от корневой директории
                relative_to_root = file_path.relative_to(root_dir)
                with open(file_path, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    # Записываем заголовок с относительным путем от корня
                    outfile.write(f"# Содержимое файла: {relative_to_root}\n")
                    outfile.write(content)
                    outfile.write("\n\n")  # Добавляем разделитель между файлами
            except FileNotFoundError:
                print(f"Файл {file_path} не найден.")
            except ValueError:
                print(f"Файл {file_path} находится вне корневой директории {root_dir}.")
            except Exception as e:
                print(f"Ошибка при чтении файла {file_path}: {e}")

if __name__ == "__main__":
    main()