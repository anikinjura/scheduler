"""
test_file_utils.py

Тесты для file_utils.
"""

import json
from pathlib import Path
import tempfile
from scheduler_runner.tasks.reports.utils.file_utils import (
    normalize_pvz_for_filename,
    find_report_file,
    find_latest_report_file,
    load_json_file,
    list_report_files
)


def test_normalize_pvz_for_filename():
    """Тест нормализации имени ПВЗ."""
    # Тест с кириллическим именем
    result = normalize_pvz_for_filename("Москва ПВЗ")
    # Ожидаем, что результат будет в транслитерированном виде
    # но точное значение может отличаться в зависимости от реализации транслитерации
    assert "moskva" in result.lower()
    assert "pvz" in result.lower()
    
    # Тест с латинским именем
    result = normalize_pvz_for_filename("Test PVZ")
    # Для латинских имен транслитерация не применяется, но может быть приведение к нижнему регистру
    assert result.lower() == "test pvz"
    
    # Тест с пустым именем
    result = normalize_pvz_for_filename("")
    assert result == ""
    
    # Тест с None
    result = normalize_pvz_for_filename(None)
    assert result == ""
    
    # Тест с именем, содержащим специальные символы
    result = normalize_pvz_for_filename("Москва-Сити ПВЗ")
    # Ожидаем транслитерацию, но регистр может сохраняться
    assert "moskva" in result.lower()
    assert "siti" in result.lower()
    assert "pvz" in result.lower()
    assert "-" in result  # дефис должен сохраниться


def test_find_report_file_existing(temp_report_dir):
    """Тест поиска существующего файла."""
    # Создаем тестовый файл
    test_file = temp_report_dir / "test_report_20260105.json"
    test_file.write_text('{"test": "data"}', encoding='utf-8')
    
    result = find_report_file(
        pattern_template="test_report_{date}.json",
        directory=temp_report_dir,
        date="2026-01-05"
    )
    
    assert result is not None
    assert result == test_file


def test_find_report_file_nonexistent(temp_report_dir):
    """Тест поиска несуществующего файла."""
    result = find_report_file(
        pattern_template="nonexistent_{date}.json",
        directory=temp_report_dir,
        date="2026-01-05"
    )
    
    assert result is None


def test_find_report_file_with_pvz(temp_report_dir):
    """Тест поиска файла с PVZ ID."""
    # Создаем тестовый файл с транслитерированным PVZ (как его создаст функция)
    # Транслитерация 'Москва ПВЗ' дает 'Moskva PVZ', но в шаблоне используется normalize_pvz_for_filename
    # который может приводить к нижнему регистру и заменять пробелы
    from scheduler_runner.tasks.reports.utils.file_utils import normalize_pvz_for_filename
    normalized_pvz = normalize_pvz_for_filename("Москва ПВЗ")
    test_file = temp_report_dir / f"test_report_{normalized_pvz}_20260105.json"
    test_file.write_text('{"test": "data"}', encoding='utf-8')

    result = find_report_file(
        pattern_template="test_report_{pvz_id}_{date}.json",
        directory=temp_report_dir,
        date="2026-01-05",
        pvz_id="Москва ПВЗ"
    )

    assert result is not None
    assert result == test_file


def test_find_report_file_invalid_pattern(temp_report_dir):
    """Тест поиска с неверным шаблоном."""
    result = find_report_file(
        pattern_template="test_report_{invalid_placeholder}_{date}.json",
        directory=temp_report_dir,
        date="2026-01-05"
    )

    assert result is None


def test_find_report_file_directory_not_exists():
    """Тест поиска в несуществующей директории."""
    fake_dir = Path("/fake/directory/that/does/not/exist")
    
    result = find_report_file(
        pattern_template="test_{date}.json",
        directory=fake_dir,
        date="2026-01-05"
    )
    
    assert result is None


def test_load_json_file_existing(temp_report_dir):
    """Тест загрузки существующего JSON файла."""
    test_file = temp_report_dir / "test.json"
    test_data = {"key": "value", "number": 42}
    
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
    
    result = load_json_file(test_file)
    
    assert result == test_data


def test_load_json_file_nonexistent():
    """Тест загрузки несуществующего файла."""
    fake_file = Path("/fake/path/file.json")
    
    result = load_json_file(fake_file)
    
    assert result is None


def test_load_json_file_invalid_json(temp_report_dir):
    """Тест загрузки файла с невалидным JSON."""
    test_file = temp_report_dir / "invalid.json"
    test_file.write_text('{"invalid": json', encoding='utf-8')
    
    result = load_json_file(test_file)
    
    assert result is None


def test_load_json_file_not_dict(temp_report_dir):
    """Тест загрузки файла, который не содержит словарь."""
    test_file = temp_report_dir / "not_dict.json"
    test_file.write_text('["array", "not", "dict"]', encoding='utf-8')
    
    result = load_json_file(test_file)
    
    assert result is None


def test_list_report_files_empty_directory(temp_report_dir):
    """Тест списка файлов в пустой директории."""
    result = list_report_files(temp_report_dir)
    
    assert result == []


def test_list_report_files_with_pattern(temp_report_dir):
    """Тест списка файлов с шаблоном."""
    # Создаем несколько файлов
    file1 = temp_report_dir / "test1.json"
    file2 = temp_report_dir / "test2.json"
    file3 = temp_report_dir / "other.txt"
    
    file1.write_text('{"test": 1}')
    file2.write_text('{"test": 2}')
    file3.write_text('other content')
    
    result = list_report_files(temp_report_dir, pattern="*.json")
    
    assert len(result) == 2
    assert file1 in result
    assert file2 in result
    assert file3 not in result


def test_find_latest_report_file_existing(create_test_files):
    """Тест поиска последнего отчета."""
    result = find_latest_report_file(
        pattern_template="ozon_giveout_report_{pvz_id}_{date}.json",
        directory=create_test_files['directory'],
        pvz_id="testpvz"
    )
    
    assert result is not None
    file_path, date = result
    assert file_path.exists()
    assert date == "2026-01-05"


def test_find_latest_report_file_nonexistent(temp_report_dir):
    """Тест поиска последнего отчета, когда файлы не существуют."""
    result = find_latest_report_file(
        pattern_template="nonexistent_{date}.json",
        directory=temp_report_dir,
        pvz_id="test"
    )
    
    assert result is None


def test_load_json_file_with_unicode(temp_report_dir):
    """Тест загрузки JSON файла с юникодом."""
    test_file = temp_report_dir / "unicode.json"
    test_data = {"русский": "текст", "special": "символы"}
    
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
    
    result = load_json_file(test_file)
    
    assert result == test_data
    assert result["русский"] == "текст"