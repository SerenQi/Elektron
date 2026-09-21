#!/usr/bin/env python3
"""
迁移脚本：将 dynamic/ 下的平铺记忆桶文件重组为域子目录结构。

旧结构: dynamic/{bucket_id}.md
新结构: dynamic/{primary_domain}/{name}_{bucket_id}.md

纯标准库，无外部依赖。
"""

import os
import re
import shutil


def _resolve_vault_dir() -> str:
    """
    Resolve the bucket vault root.
    Priority: $OMBRE_BUCKETS_DIR > config.yaml > built-in ./buckets.
    """
    env_dir = os.environ.get("OMBRE_BUCKETS_DIR", "").strip()
    if env_dir:
        return os.path.expanduser(env_dir)
    try:
        from utils import load_config
        return load_config()["buckets_dir"]
    except Exception:
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "buckets"
        )


VAULT_DIR = _resolve_vault_dir()
DYNAMIC_DIR = os.path.join(VAULT_DIR, "dynamic")


def sanitize_name(name: str) -> str:
    cleaned = re.sub(r"[^\w\s\u4e00-\u9fff-]", "", name, flags=re.UNICODE)
    return cleaned.strip()[:80] or "unnamed"


def parse_frontmatter(filepath):
    """纯正则解析 YAML frontmatter 中的 id, name, domain 字段。"""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    if not content.startswith("---"):
        return None
    parts = content.split("---", 2)
    if len(parts) < 3:
        return None
    yaml_text = parts[1]

    meta = {}
    # 提取 id
    m = re.search(r"^id:\s*(.+)$", yaml_text, re.MULTILINE)
    if m:
        meta["id"] = m.group(1).strip().strip("'\"")
    # 提取 name
    m = re.search(r"^name:\s*(.+)$", yaml_text, re.MULTILINE)
    if m:
        meta["name"] = m.group(1).strip().strip("'\"")
    # 提取 domain 列表
    m = re.search(r"^domain:\s*\n((?:\s*-\s*.+\n?)+)", yaml_text, re.MULTILINE)
    if m:
        meta["domain"] = re.findall(r"-\s*(.+)", m.group(1))
    else:
        meta["domain"] = ["未分类"]

    return meta


def migrate():
    if not os.path.exists(DYNAMIC_DIR):
        print(f"目录不存在: {DYNAMIC_DIR}")
        return

    # 只处理直接在 dynamic/ 下的 .md 文件（不处理已在子目录中的）
    files = [f for f in os.listdir(DYNAMIC_DIR)
             if f.endswith(".md") and os.path.isfile(os.path.join(DYNAMIC_DIR, f))]

    if not files:
        print("没有需要迁移的文件。")
        return

    print(f"发现 {len(files)} 个待迁移文件\n")

    for filename in sorted(files):
        old_path = os.path.join(DYNAMIC_DIR, filename)
        try:
            meta = parse_frontmatter(old_path)
        except Exception as e:
            print(f"  ✗ 无法解析 {filename}: {e}")
            continue

        if not meta:
            print(f"  ✗ 无 frontmatter: {filename}")
            continue

        bucket_id = meta.get("id", filename.replace(".md", ""))
        name = meta.get("name", "")
        domain = meta.get("domain", ["未分类"])
        primary_domain = sanitize_name(domain[0]) if domain else "未分类"

        # 构造新路径
        domain_dir = os.path.join(DYNAMIC_DIR, primary_domain)
        os.makedirs(domain_dir, exist_ok=True)

        if name and name != bucket_id:
            new_filename = f"{sanitize_name(name)}_{bucket_id}.md"
        else:
            new_filename = f"{bucket_id}.md"

        new_path = os.path.join(domain_dir, new_filename)

        # 移动
        shutil.move(old_path, new_path)
        print(f"  ✓ {filename}")
        print(f"    → {primary_domain}/{new_filename}")

    print("\n迁移完成。")

    # 展示新结构
    print("\n=== 新目录结构 ===")
    for root, dirs, files in os.walk(DYNAMIC_DIR):
        level = root.replace(DYNAMIC_DIR, "").count(os.sep)
        indent = "  " * level
        folder = os.path.basename(root)
        if level > 0:
            print(f"{indent}📁 {folder}/")
        for f in sorted(files):
            if f.endswith(".md"):
                print(f"{indent}  📄 {f}")


if __name__ == "__main__":
    migrate()
