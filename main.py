import sqlparse
import re
import os
from textwrap import indent
import json

# 比較演算子のマッピング
comparison_operators = {
    '>=': 'gte',
    '<=': 'lte',
    '>': 'gt',
    '<': 'lt',
    '!=': 'ne',
    '==': 'eq',
    '=': 'eq'
}

# MongoDB更新演算子のマッピング
mongo_operators = {
    '$set': 'set',
    '$inc': 'inc',
    '$min': 'min',
    '$max': 'max',
    '$mul': 'mul',
    '$rename': 'rename',
    '$setOnInsert': 'setOnInsert',
    '$unset': 'unset',
    '$currentDate': 'currentDate'
}

# 演算子のマッピング（従来の式用）
set_operators = {
    '+': 'inc',
    '-': 'inc',
    '*': 'mul',
    '/': 'mul'
}

#hash unique ascending

# コレクション情報（補完）
collection_info = {
    "users": {
        "column_list": [
            {"variable_type": "String", "variable_name": "name", "variable_explanation": "ユーザー名", "index_type": "hash", "none": False},
            {"variable_type": "String", "variable_name": "email", "variable_explanation": "メールアドレス", "index_type": "none", "is_array": False},
            {"variable_type": "int", "variable_name": "id", "variable_explanation": "ユーザーID", "index_type": "none", "is_array": False},
            {"variable_type": "double", "variable_name": "balance", "variable_explanation": "残高", "index_type": "none", "is_array": False},
            {"variable_type": "String", "variable_name": "hobbies", "variable_explanation": "趣味の配列", "index_type": "none", "is_array": True},  # 文字列配列
            {"variable_type": "double", "variable_name": "ratings", "variable_explanation": "評価の配列", "index_type": "none", "is_array": True}  # 浮動小数点配列
        ],
        "queries": [
            #{"query": "UPDATE users SET hobbies = arg1 WHERE id = arg2;", "method_name": "setHobbiesById"},
            #{"query": "UPDATE users SET hobbies = hobbies + arg1 WHERE id = arg2 AND hobbies ALL arg3;", "method_name": "removeHobbyById"},
            #{"query": "SELECT hobbies FROM users WHERE id = arg1 AND hobbies > 'a';", "method_name": "findHobbiesByIdFiltered"},
            #{"query": "SELECT hobbies FROM users WHERE id = arg1 AND hobbies = ANY(arg2);", "method_name": "findHobbiesByIdFilteredTest"},
            {"query": "INSERT INTO users (id, hobbies) VALUES (arg1, arg2);", "method_name": "insertUserWithHobbies"},
            #{"query": "DELETE FROM users WHERE id = arg1 AND hobbies = arg2;", "method_name": "deleteUserByIdAndHobby"},
            #{"query": "UPDATE users SET ratings = arg1 WHERE id = arg2 AND ratings >= arg3? AND ratings <= arg4?;", "method_name": "setRatingsById"},
            #{"query": "UPDATE users SET ratings = ratings - arg2 WHERE id = arg1;", "method_name": "removeRatingById"},
            #{"query": "SELECT ratings FROM users WHERE id = arg1 AND ratings > 5.0;", "method_name": "findRatingsByIdFiltered"},
            #{"query": "SELECT ratings FROM users WHERE id = arg1 AND ratings > arg2;", "method_name": "findRatingsByIdFilteredTest"},
            #{"query": "INSERT INTO users (id, ratings) VALUES (arg1, arg2);", "method_name": "insertUserWithRatings"},
            #{"query": "DELETE FROM users WHERE id = arg1 AND ratings = arg2;", "method_name": "deleteUserByIdAndRating"}
        ]
    }
}

# コレクション情報から引数の型を取得
def get_arg_type(collection, arg_name, sql, collection_info):
    arg_name = arg_name.replace(";","").strip()
    if collection not in collection_info:
        return "Object"

    # SET句、INSERT句、WHERE句からフィールドと引数の対応を抽出
    set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
    insert_clause = re.search(r'INSERT INTO \w+\s*\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
    where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)

    # Java型マッピング
    type_mapping = {
        "String": "String",
        "int": "Integer",
        "double": "Double",
        "Date": "Date"
    }
    
    # 配列操作の特別判定
    def is_array_operation(value):
        """値が配列操作を含むか判定"""
        return any(op in value for op in [' + ', ' - ', ' ALL ', ' IN ', ' ANY('])

    # SET句の解析
    if set_clause:
        for set_item in set_clause.group(1).split(','):
            field, value = [x.strip() for x in set_item.split('=')]
            value = value.rstrip(';').strip()
            
            # arg{数値} を抽出
            arg_match = re.search(r'arg\d+', value)
            if arg_match:
                value = arg_match.group(0)
                if value == arg_name:
                    for col in collection_info[collection]["column_list"]:
                        if col["variable_name"] == field:
                            element_type = col["variable_type"]
                            
                            # 配列操作が含まれている場合
                            if is_array_operation(set_item):
                                # 配列操作の場合、要素の型を返す
                                return type_mapping.get(element_type, "Object")
                            
                            if col.get("is_array", False):
                                return f"List<{type_mapping.get(element_type, 'Object')}>"
                            return type_mapping.get(element_type, "Object")

    # INSERT句の解析
    if insert_clause:
        fields = [f.strip() for f in insert_clause.group(1).split(',')]
        values = [v.strip() for v in insert_clause.group(2).split(',')]
        for field, value in zip(fields, values):
            value = value.rstrip(';').strip()
            
            # arg{数値} を抽出
            arg_match = re.search(r'arg\d+', value)
            if arg_match:
                value = arg_match.group(0)
                if value == arg_name:
                    for col in collection_info[collection]["column_list"]:
                        if col["variable_name"] == field:
                            element_type = col["variable_type"]
                            if col.get("is_array", False):
                                return f"List<{type_mapping.get(element_type, 'Object')}>"
                            return type_mapping.get(element_type, "Object")

    # WHERE句の解析（強化版）
    if where_clause:
        # 条件をAND/ORで分割
        conditions = re.split(r'\bAND\b|\bOR\b', where_clause.group(1), flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            
            # ALL演算子の処理
            if ' ALL ' in condition:
                field, value = [x.strip() for x in condition.split(' ALL ', 1)]
                value = value.rstrip(';').strip()
                
                # arg{数値} を抽出
                arg_match = re.search(r'arg\d+', value)
                if arg_match and arg_match.group(0) == arg_name:
                    # ALL演算子の値は常に配列
                    for col in collection_info[collection]["column_list"]:
                        if col["variable_name"] == field:
                            element_type = col["variable_type"]
                            return f"List<{type_mapping.get(element_type, 'Object')}>"
                    return "List<Object>"
            
            # IN演算子の処理
            if ' IN ' in condition.upper():
                field, value = [x.strip() for x in condition.split(' IN ', 1)]
                value = value.rstrip(';').strip()
                
                # arg{数値} を抽出
                arg_match = re.search(r'arg\d+', value)
                if arg_match and arg_match.group(0) == arg_name:
                    # IN演算子の値は常に配列
                    for col in collection_info[collection]["column_list"]:
                        if col["variable_name"] == field:
                            element_type = col["variable_type"]
                            return f"List<{type_mapping.get(element_type, 'Object')}>"
                    return "List<Object>"
            
            # ANY演算子の処理
            if '= ANY(' in condition.upper():
                field, value = [x.strip() for x in condition.split('= ANY(', 1)]
                value = value.rstrip(')').strip()
                
                # arg{数値} を抽出
                arg_match = re.search(r'arg\d+', value)
                if arg_match and arg_match.group(0) == arg_name:
                    # ANY演算子の値は常に配列
                    for col in collection_info[collection]["column_list"]:
                        if col["variable_name"] == field:
                            element_type = col["variable_type"]
                            return f"List<{type_mapping.get(element_type, 'Object')}>"
                    return "List<Object>"
            
            # 通常の比較演算子
            for op in comparison_operators.keys():
                if op in condition:
                    parts = condition.split(op, 1)
                    if len(parts) < 2:
                        continue
                    
                    field, value = [x.strip() for x in parts]
                    value = value.rstrip(';').strip()
                    
                    # arg{数値} を抽出
                    arg_match = re.search(r'arg\d+', value)
                    if arg_match:
                        value = arg_match.group(0)
                        if value == arg_name:
                            for col in collection_info[collection]["column_list"]:
                                if col["variable_name"] == field:
                                    element_type = col["variable_type"]
                                    if col.get("is_array", False):
                                        # 配列フィールドの比較は単一要素
                                        return type_mapping.get(element_type, "Object")
                                    return type_mapping.get(element_type, "Object")

    return "Object"

def clean_value(value):
    """値から不要なセミコロンや空白を除去"""
    return value.rstrip(';').strip()

# WHERE句を解析してMongoDBフィルターに変換
def parse_where_clause(where_clause, collection_info, collection, auto_index=True, is_with_data=False):
    if not where_clause:
        return 'new Document()'

    conditions = []
    current_condition = ""
    parenthesis_level = 0
    i = 0
    while i < len(where_clause):
        char = where_clause[i]
        if char == '(':
            parenthesis_level += 1
        elif char == ')':
            parenthesis_level -= 1
        elif parenthesis_level == 0 and char == ' ' and i + 3 < len(where_clause):
            next_part = where_clause[i+1:i+4].upper()
            if next_part in ['AND', 'OR ', 'XOR']:
                if current_condition:
                    conditions.append(current_condition.strip())
                    current_condition = ""
                i += 4 if next_part != 'OR ' else 3
                continue
        current_condition += char
        i += 1
    if current_condition:
        conditions.append(current_condition.strip())

    # 条件を再分割して論理演算子を正確に処理
    final_conditions = []
    logical_operators = []
    for condition in conditions:
        parts = []
        current = ""
        paren_level = 0
        i = 0
        while i < len(condition):
            char = condition[i]
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
            elif paren_level == 0 and char == ' ' and i + 3 < len(condition):
                next_part = condition[i+1:i+4].upper()
                if next_part in ['AND', 'OR ', 'XOR']:
                    if current:
                        parts.append((current.strip(), next_part.strip()))
                        current = ""
                    i += 4 if next_part != 'OR ' else 3
                    continue
            current += char
            i += 1
        if current:
            parts.append((current.strip(), None))
        for cond, op in parts:
            final_conditions.append(cond)
            if op:
                logical_operators.append(op.lower())

    # フィルタ生成
    filters = []
    for condition in final_conditions:
        # ALLキーワードの処理（配列が指定値の全てを含む）
        if ' ALL ' in condition:
            field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
            filters.append(f'Filters.all("{field}", {value})')
            continue
            
        # INキーワードの処理（配列が指定値のいずれかを含む）
        if ' IN ' in condition.upper():
            field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
            filters.append(f'Filters.in("{field}", {value})')
            continue
            
        # ANYキーワードの処理（いずれかにヒット）
        if '= ANY(' in condition.upper():
            field, value = [clean_value(x) for x in condition.split('= ANY(', 1)]
            value = value.rstrip(')').strip()
            filters.append(f'Filters.in("{field}", {value})')
            continue
            
        # その他の比較演算子
        for op, mongo_op in comparison_operators.items():
            if op in condition:
                field, value = [clean_value(x) for x in condition.split(op, 1)]
                
                # カラム情報を取得
                field_info = next((col for col in collection_info[collection]["column_list"] 
                                 if col["variable_name"] == field), None)
                if not field_info:
                    continue
                    
                element_type = field_info.get("element_type", field_info["variable_type"])
                is_array = field_info.get("is_array", False)

                # 配列フィールドの場合
                if is_array:
                    if element_type in ["int", "double"]:
                        # 数値配列 → $elemMatch + 数値比較
                        filters.append(f'Filters.elemMatch("{field}", Filters.{mongo_op}("{field}", {value}))')
                    elif element_type == "String":
                        # 文字列配列
                        if op == '=' or op == '==':
                            # 完全一致（$allを使用）
                            filters.append(f'Filters.all("{field}", {value})')
                        elif op == 'LIKE':
                            # 部分一致（正規表現）
                            filters.append(f'Filters.elemMatch("{field}", Filters.regex("{field}", {value}))')
                        else:
                            # その他の比較
                            filters.append(f'Filters.elemMatch("{field}", Filters.{mongo_op}("{field}", {value}))')
                else:
                    # 非配列フィールド
                    filters.append(f'Filters.{mongo_op}("{field}", {value})')
                break

    # インデックスの自動追加
    if auto_index:
        for col in collection_info[collection]["column_list"]:
            if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                field = col["variable_name"]
                # 既にそのフィールドに対する条件が無い場合のみ追加
                if not any(f'"{field}"' in f for f in filters):
                    filters.append(f'Filters.exists("{field}")')

    # フィルタの結合処理
    if not filters:
        return 'new Document()'
    
    # 論理演算子の結合
    if logical_operators:
        # XORの処理
        if 'xor' in logical_operators:
            xor_index = logical_operators.index('xor')
            a = filters[xor_index]
            b = filters[xor_index + 1]
            xor_expr = f'Filters.or(Filters.and({a}, Filters.not({b})), Filters.and(Filters.not({a}), {b}))'
            filters = filters[:xor_index] + [xor_expr] + filters[xor_index+2:]
            logical_operators.pop(xor_index)
        
        # AND/ORの結合
        if filters:
            combined_filter = filters[0]
            for i in range(1, len(filters)):
                op = logical_operators[i-1] if i-1 < len(logical_operators) else 'and'
                combined_filter = f'Filters.{op}({combined_filter}, {filters[i]})'
            return combined_filter
    
    # フィルタが1つの場合
    if len(filters) == 1:
        return filters[0]
    
    filters = [s.replace("?","") for s in filters]
    
    # デフォルトはANDで結合
    return f'Filters.and({", ".join(filters)})'

# UserCollectionData からフィルターを生成
def generate_filter_from_user_collection_data(collection_info, prefix="update", collection="users"):
    filters = []
    for col in collection_info[collection]["column_list"]:
        field = col["variable_name"]
        java_code = f'        if ({prefix}.is{field.capitalize()}Flag()) {{'
        java_code += f'\n            filters.add(Filters.eq("{field}", {prefix}.get{field.capitalize()}()));'
        java_code += '\n        }'
        filters.append(java_code)
    return filters

# バルク操作を生成
def generate_bulk_operations(collection_info):
    java_code = []
    collection = next(iter(collection_info))
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    java_code.append(f'public static boolean bulkInsert{collection.capitalize()}(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList) {{')
    java_code.append('    try {')
    java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
    java_code.append('        List<Document> documents = new ArrayList<>();')
    java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList)' + '{')
    java_code.append('            documents.add(data.toDocument());')
    java_code.append('        }')
    java_code.append('        collection.insertMany(documents);')
    java_code.append('        return true;')
    java_code.append('    } catch (com.mongodb.MongoWriteException e) {')
    java_code.append('        if (e.getCode() == 11000) {')
    java_code.append('            return false;')
    java_code.append('        }')
    java_code.append('        throw e;')
    java_code.append('    } catch (Exception e) {')
    java_code.append('        return false;')
    java_code.append('    }')
    java_code.append('}')
    

    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    java_code.append(f'public static boolean bulkUpdate{collection.capitalize()}(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList) {{')
    java_code.append('    try {')
    java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
    java_code.append('        List<WriteModel<Document>> updates = new ArrayList<>();')
    java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList)' + '{')
    java_code.append('            List<Bson> filters = new ArrayList<>();')
    for col in collection_info[collection]["column_list"]:
        field = col["variable_name"]
        java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
        java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
        java_code.append('            }')
    java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
    java_code.append('            List<Bson> updateOps = new ArrayList<>();')
    for col in collection_info[collection]["column_list"]:
        field = col["variable_name"]
        java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
        java_code.append(f'                updateOps.add(Updates.set("{field}", data.get{field.capitalize()}()));')
        java_code.append('            }')
    java_code.append('            if (!updateOps.isEmpty()) {')
    java_code.append('                updates.add(new UpdateManyModel<>(filter, Updates.combine(updateOps)));')
    java_code.append('            }')
    java_code.append('        }')
    java_code.append('        if (!updates.isEmpty()) {')
    java_code.append('            BulkWriteResult result = collection.bulkWrite(updates);')
    java_code.append('            return result.getModifiedCount() > 0;')
    java_code.append('        }')
    java_code.append('        return false;')
    java_code.append('    } catch (Exception e) {')
    java_code.append('        return false;')
    java_code.append('    }')
    java_code.append('}')

    return java_code

# SQLを解析してMongoDB用Javaコードを生成（単一引数版）
def parse_sql_to_mongodb_single(sql, method_name, collection_info, auto_index=True,is_transaction = False):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    # 引数の型をcollection_infoから正確に取得
    fields = re.search(r'\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
    if fields:
        field_list = [f.strip() for f in fields.group(1).split(',')]
        arg_params = ', '.join(
            f'{next(col["variable_type"] for col in collection_info[collection]["column_list"] if col["variable_name"] == field)} {args[i]}'
            for i, field in enumerate(field_list)
        )
    else:
        arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    if operation == "insert":
        fields = re.search(r'\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
        if fields:
            field_list = [f.strip() for f in fields.group(1).split(',')]
            args = [a.strip() for a in fields.group(2).split(',')]
            java_code.append(f'public static boolean {method_name}{"Transaction" if is_transaction else ""}(MongoDatabase db, {"ClientSession session," if is_transaction else ""}{arg_params}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append(f'        {collection.capitalize()}CollectionData data = new {collection.capitalize()}CollectionData();')
            for field, arg in zip(field_list, args):
                java_code.append(f'        data.set{field.capitalize()}({arg});')
            java_code.append(f'        collection.insertOne({"session," if is_transaction else ""}data.toDocument());')
            java_code.append('        return true;')
            java_code.append('    } catch (com.mongodb.MongoWriteException e) {')
            java_code.append('        if (e.getCode() == 11000) {')
            java_code.append('            return false;')
            java_code.append('        }')
            java_code.append('        throw e;')
            java_code.append('    } catch (Exception e) {')
            java_code.append('        return false;')
            java_code.append('    }')
            java_code.append('}')

    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static boolean {method_name}{"Transaction" if is_transaction else ""}(MongoDatabase db, {"ClientSession session," if is_transaction else ""}{arg_params}) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        if set_clause:
            updates = []
            for set_item in set_clause.group(1).split(','):
                field, value = [x.strip() for x in set_item.split('=')]
                mongo_op_match = re.match(r'\$(\w+)\((.*?)\)', value.strip())
                field_info = next((col for col in collection_info[collection]["column_list"] 
                    if col["variable_name"] == field), None)
                is_array = field_info.get("is_array", False) if field_info else False
                
                # 配列操作の特別処理
                if is_array and ' - ' in value:
                    # 配列から要素を削除 ($pull)
                    _, element = value.split(' - ')
                    updates.append(f'Updates.pull("{field}", {element.strip()})')
                    continue
                    
                if is_array and ' + ' in value:
                    # 配列に要素を追加 ($push)
                    _, element = value.split(' + ')
                    updates.append(f'Updates.push("{field}", {element.strip()})')
                    continue
                
                
                
                if mongo_op_match:
                    op, arg = mongo_op_match.groups()
                    op = f'${op}'
                    if op in mongo_operators:
                        if op == '$currentDate':
                            updates.append(f'Updates.currentDate("{field}")')
                        elif op == '$unset':
                            updates.append(f'Updates.unset("{field}")')
                        elif op == '$rename':
                            updates.append(f'Updates.rename("{field}", "{arg}")')
                        else:
                            updates.append(f'Updates.{mongo_operators[op]}("{field}", {arg})')
                else:
                    for op, mongo_op in set_operators.items():
                        if op in value:
                            left, right = value.split(op)
                            left, right = left.strip(), right.strip()
                            if left == field:
                                if op == '-':
                                    updates.append(f'Updates.inc("{field}", -{right})')
                                elif op == '/':
                                    updates.append(f'Updates.mul("{field}", 1.0 / {right})')
                                else:
                                    updates.append(f'Updates.{mongo_op}("{field}", {right})')
                                break
                    else:
                        updates.append(f'Updates.set("{field}", {value})')
            java_code.append(f'        Bson update = Updates.combine({", ".join(updates)});')

        if where_clause:
            filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index)
            java_code.append(f'        Bson filter = {filters};')
            java_code.append(f'        UpdateResult result = collection.updateOne({"session," if is_transaction else ""}filter, update);')
            java_code.append('        return result.getMatchedCount() > 0;')
        else:
            java_code.append(f'        UpdateResult result = collection.updateOne({"session," if is_transaction else ""}new Document(), update);')
            java_code.append('        return result.getMatchedCount() > 0;')
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static boolean {method_name}{"Transaction" if is_transaction else ""}(MongoDatabase db, {"ClientSession session," if is_transaction else ""}{arg_params}) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        if where_clause:
            filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index)
            java_code.append(f'        Bson filter = {filters};')
            java_code.append('        DeleteResult result = collection.deleteOne(filter);')
            java_code.append('        return result.getDeletedCount() > 0;')
        else:
            java_code.append(f'        DeleteResult result = collection.deleteOne({"session," if is_transaction else ""}new Document());')
            java_code.append('        return result.getDeletedCount() > 0;')
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        return_type = f'{collection.capitalize()}CollectionData' if limit_value == 1 else f'List<{collection.capitalize()}CollectionData>'
        return_value = 'null' if limit_value == 1 else 'Collections.emptyList()'

        # LIMIT 1 の場合
        if limit_value == 1:
            java_code.append(f'public static DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData> {method_name}{"Transaction" if is_transaction else ""}(MongoDatabase db, {"ClientSession session," if is_transaction else ""}{arg_params}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            if where_conditions:
                filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                java_code.append(f'        Bson filter = {filters};')
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}filter).first();')
                java_code.append('        if (doc == null) {')
                java_code.append(f'            return DataBaseResultPair.of(false, null);')
                java_code.append('        }')
                java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find().sort({"session," if is_transaction else ""}new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}).first();')
                java_code.append('        if (doc == null) {')
                java_code.append(f'            return DataBaseResultPair.of(false, null);')
                java_code.append('        }')
                java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, null);')
            java_code.append('    }')
            java_code.append('}')
        else:
            # LIMIT なしの場合、One と Many の両方を生成
            # One バージョンの生成
            java_code.append(f'public static DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData> {method_name}{"Transaction" if is_transaction else ""}One(MongoDatabase db, {"ClientSession session," if is_transaction else ""}{arg_params}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            if where_conditions:
                filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                java_code.append(f'        Bson filter = {filters};')
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}filter).first();')
                java_code.append('        if (doc == null) {')
                java_code.append(f'            return DataBaseResultPair.of(false, null);')
                java_code.append('        }')
                java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find({"session," if is_transaction else ""}).first();')
                java_code.append('        if (doc == null) {')
                java_code.append(f'            return DataBaseResultPair.of(false, null);')
                java_code.append('        }')
                java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, null);')
            java_code.append('    }')
            java_code.append('}')
            java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
            # Many バージョンの生成
            java_code.append(f'public static DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>> {method_name}{"Transaction" if is_transaction else ""}Many(MongoDatabase db, {"ClientSession session," if is_transaction else ""}{arg_params}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            if where_conditions:
                filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                java_code.append(f'        Bson filter = {filters};')
                java_code.append(f'        FindIterable<Document> results = collection.find({"session," if is_transaction else ""}filter);')
            else:
                java_code.append(f'        FindIterable<Document> results = collection.find({"session," if is_transaction else ""});')
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'        results = results.sort(new Document().append({", ".join(sort_fields)}));')
            if limit_value:
                java_code.append(f'        results = results.limit({limit_value});')
            java_code.append(f'        List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('        for (Document doc : results) {')
            java_code.append(f'            resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        }')
            java_code.append(f'        return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, Collections.emptyList());')
            java_code.append('    }')
            java_code.append('}')

    return java_code



def process_args(sql, arg_params):
    # SQLクエリから?が付いている引数名を抽出 (例: {'arg3', 'arg4'})
    optional_args = set(re.findall(r'(arg\d+)\?', sql))
    
    # 元のarg_paramsをカンマで分割して個々の引数定義に分解
    params_list = [p.strip() for p in arg_params.split(',')]
    
    # 新しいarg_params用リストと判定結果用リスト
    new_arg_params = []
    is_optional_list = False
    
    for param in params_list:
        # 引数名を抽出（最後の単語を取得）
        arg_name = param.split()[-1]
        
        # ?付き引数の場合のみ保持
        if arg_name in optional_args:
            new_arg_params.append(param)
            is_optional_list = True
    
    # 新しいarg_params文字列を生成
    new_arg_params_str = ', '.join(new_arg_params)
    
    return new_arg_params_str, is_optional_list

def is_optional_arg_present(arg_params, optional_arg_name):
    """
    オプショナル引数名が引数リストに完全一致する形で存在するか判定
    
    Args:
        arg_params (str): 引数リストの文字列（例: "Double arg3, Double arg4"）
        optional_arg_name (str): オプショナル引数名（例: "arg3?"）
        
    Returns:
        bool: オプショナル引数名（?を除いた部分）が引数リストに完全一致する形で存在すればTrue
    """
    # オプショナル引数名から?を除去（例: "arg3?" → "arg3"）
    base_name = optional_arg_name.rstrip('?')
    
    # 引数リストが空の場合はFalse
    if not arg_params:
        return False
        
    # 引数リストをカンマで分割
    param_list = [p.strip() for p in arg_params.split(',')]
    
    # 各引数定義から引数名を抽出
    for param in param_list:
        # 空白で分割し、最後の要素を引数名として取得
        parts = param.split()
        if not parts:
            continue
            
        arg_name = parts[-1]
        # 完全一致チェック
        if arg_name == base_name:
            return True
            
    return False


# SQLを解析してMongoDB用Javaコードを生成（List<UserCollectionData>引数版）
# SQLを解析してMongoDB用Javaコードを生成（UserCollectionData引数版）
def parse_sql_to_mongodb_user_collection_data(sql, method_name, collection_info, auto_index=True):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
    limit_value = int(limit_match.group(1)) if limit_match else None
    
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    new_params, optional_flag = process_args(sql, arg_params)
    
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    
    if operation == "insert":
        fields = re.search(r'\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
        if fields:
            java_code.append(f'public static boolean {method_name}WithData(MongoDatabase db, {collection.capitalize()}CollectionData data) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        collection.insertOne(data.toDocument());')
            java_code.append('        return true;')
            java_code.append('    } catch (com.mongodb.MongoWriteException e) {')
            java_code.append('        if (e.getCode() == 11000) {')
            java_code.append('            return false;')
            java_code.append('        }')
            java_code.append('        throw e;')
            java_code.append('    } catch (Exception e) {')
            java_code.append('        return false;')
            java_code.append('    }')
            java_code.append('}')

    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        
        java_code.append(f'public static boolean {method_name}WithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('        List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            if(col["index_type"] != "none"):
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
        
        java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        # SET句の解析
        updates = []
        if set_clause:
            for set_item in set_clause.group(1).split(','):
                field, value = [x.strip() for x in set_item.split('=')]
                value = clean_value(value)
                
                # フィールド情報を取得
                field_info = next((col for col in collection_info[collection]["column_list"] 
                                 if col["variable_name"] == field), None)
                is_array = field_info.get("is_array", False) if field_info else False
                
                # 配列操作の特別処理
                if is_array and ' - ' in value:
                    # 配列から要素を削除 ($pull)
                    _, element = value.split(' - ')
                    element = element.strip()
                    # UserCollectionDataから値を取得
                    updates.append(f'Updates.pull("{field}", data.get{field.capitalize()}())')
                    continue
                    
                if is_array and ' + ' in value:
                    # 配列に要素を追加 ($push)
                    _, element = value.split(' + ')
                    element = element.strip()
                    # UserCollectionDataから値を取得
                    updates.append(f'Updates.push("{field}", data.get{field.capitalize()}())')
                    continue
                
                # 通常の更新操作
                updates.append(f'Updates.set("{field}", data.get{field.capitalize()}())')
        
        java_code.append(f'        Bson updateOps = Updates.combine({", ".join(updates)});')

        # WHERE句の解析
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        value = value.replace("?","")
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'        Bson whereFilter = {where_filter};')
            java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('        UpdateResult result = collection.updateOne(combinedFilter, updateOps);')
            java_code.append('        return result.getMatchedCount() > 0;')
        else:
            java_code.append('        UpdateResult result = collection.updateOne(filter, updateOps);')
            java_code.append('        return result.getMatchedCount() > 0;')
        
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static boolean {method_name}WithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('        List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            if(col["index_type"] != "none"):
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
        
        java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}",{value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'        Bson whereFilter = {where_filter};')
            java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('        DeleteResult result = collection.deleteOne(combinedFilter);')
            java_code.append('        return result.getDeletedCount() > 0;')
        else:
            java_code.append('        DeleteResult result = collection.deleteOne(filter);')
            java_code.append('        return result.getDeletedCount() > 0;')
        
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        return_type = f'{collection.capitalize()}CollectionData' if limit_value == 1 else f'List<{collection.capitalize()}CollectionData>'
        return_value = 'null' if limit_value == 1 else 'Collections.emptyList()'

        # LIMIT 1 の場合
        if limit_value == 1:
            java_code.append(f'public static DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData> {method_name}WithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if(col["index_type"] != "none"):
                    field = col["variable_name"]
                    java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('            }')
            
            java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            # UserCollectionDataから値を取得
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'        Bson whereFilter = {where_filter};')
                java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find(combinedFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find(combinedFilter).first();')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find(filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find(filter).first();')
            
            java_code.append('        if (doc == null) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, null);')
            java_code.append('    }')
            java_code.append('}')
        else:
            # LIMIT なしの場合、One と Many の両方を生成
            # One バージョンの生成
            java_code.append(f'public static DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData> {method_name}OneWithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if(col["index_type"] != "none"):
                    field = col["variable_name"]
                    java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('            }')
            
            java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.all("{field}",{value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            # UserCollectionDataから値を取得
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'        Bson whereFilter = {where_filter};')
                java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find(combinedFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find(combinedFilter).first();')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find(filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find(filter).first();')
            
            java_code.append('        if (doc == null) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, null);')
            java_code.append('    }')
            java_code.append('}')
            
            # Many バージョンの生成
            java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
            java_code.append(f'public static DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>> {method_name}ManyWithData(MongoDatabase db, {collection.capitalize()}CollectionData data) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if(col["index_type"] != "none"):
                    field = col["variable_name"]
                    java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('            }')
            
            java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            # UserCollectionDataから値を取得
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'        Bson whereFilter = {where_filter};')
                java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('        FindIterable<Document> results = collection.find(combinedFilter);')
            else:
                java_code.append('        FindIterable<Document> results = collection.find(filter);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'        results = results.sort(new Document().append({", ".join(sort_fields)}));')
            
            if limit_value:
                java_code.append(f'        results = results.limit({limit_value});')
            
            java_code.append(f'        List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('        for (Document doc : results) {')
            java_code.append(f'            resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        }')
            java_code.append(f'        return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, Collections.emptyList());')
            java_code.append('    }')
            java_code.append('}')

    return java_code

# SQLを解析してMongoDB用Javaコードを生成（List<UserCollectionData>引数版）
def parse_sql_to_mongodb_list_user_collection_data(sql, method_name, collection_info, auto_index=True):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
    limit_value = int(limit_match.group(1)) if limit_match else None
    
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    new_params, optional_flag = process_args(sql, arg_params)

    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    if operation == "insert":
        java_code.append(f'public static boolean {method_name}WithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('        List<Document> documents = new ArrayList<>();')
        java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
        java_code.append('            documents.add(data.toDocument());')
        java_code.append('        }')
        java_code.append('        collection.insertMany(documents);')
        java_code.append('        return true;')
        java_code.append('    } catch (com.mongodb.MongoWriteException e) {')
        java_code.append('        if (e.getCode() == 11000) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('        throw e;')
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static boolean {method_name}WithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('        List<WriteModel<Document>> updates = new ArrayList<>();')
        java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
        java_code.append('            List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            if(col["index_type"] != "none"):
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
        
        java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        # SET句の解析
        if set_clause:
            set_updates = []
            for set_item in set_clause.group(1).split(','):
                field, value = [x.strip() for x in set_item.split('=')]
                value = clean_value(value)
                
                # フィールド情報を取得
                field_info = next((col for col in collection_info[collection]["column_list"] 
                                 if col["variable_name"] == field), None)
                is_array = field_info.get("is_array", False) if field_info else False
                
                # 配列操作の特別処理
                if is_array and ' - ' in value:
                    # 配列から要素を削除 ($pull)
                    _, element = value.split(' - ')
                    element = element.strip()
                    # UserCollectionDataから値を取得
                    set_updates.append(f'Updates.pull("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                    
                if is_array and ' + ' in value:
                    # 配列に要素を追加 ($push)
                    _, element = value.split(' + ')
                    element = element.strip()
                    # UserCollectionDataから値を取得
                    set_updates.append(f'Updates.push("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # 通常の更新操作
                set_updates.append(f'Updates.set("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
            
            java_code.append(f'            Bson updateOps = Updates.combine({", ".join(set_updates)});')
        else:
            java_code.append('            Bson updateOps = new Document();')

        # WHERE句の解析
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}",{value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'            Bson whereFilter = {where_filter};')
            java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('            updates.add(new UpdateManyModel<>(combinedFilter, updateOps));')
        else:
            java_code.append('            updates.add(new UpdateManyModel<>(filter, updateOps));')
        
        java_code.append('        }')
        java_code.append('        if (!updates.isEmpty()) {')
        java_code.append('            BulkWriteResult result = collection.bulkWrite(updates);')
        java_code.append('            return result.getModifiedCount() > 0;')
        java_code.append('        }')
        java_code.append('        return false;')
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static boolean {method_name}WithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    try {')
        java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('        List<WriteModel<Document>> deletes = new ArrayList<>();')
        java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
        java_code.append('            List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            field = col["variable_name"]
            java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
            java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
            java_code.append('            }')
        
        java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    # UserCollectionDataから値を取得
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'            Bson whereFilter = {where_filter};')
            java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('            deletes.add(new DeleteManyModel<>(combinedFilter));')
        else:
            java_code.append('            deletes.add(new DeleteManyModel<>(filter));')
        
        java_code.append('        }')
        java_code.append('        if (!deletes.isEmpty()) {')
        java_code.append('            BulkWriteResult result = collection.bulkWrite(deletes);')
        java_code.append('            return result.getDeletedCount() > 0;')
        java_code.append('        }')
        java_code.append('        return false;')
        java_code.append('    } catch (Exception e) {')
        java_code.append('        return false;')
        java_code.append('    }')
        java_code.append('}')

    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        return_type = f'{collection.capitalize()}CollectionData' if limit_value == 1 else f'List<{collection.capitalize()}CollectionData>'
        return_value = 'null' if limit_value == 1 else 'Collections.emptyList()'

        # LIMIT 1 の場合
        if limit_value == 1:
            java_code.append(f'public static DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData> {method_name}WithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        List<Bson> allFilters = new ArrayList<>();')
            java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
            java_code.append('            List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
            
            java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            # UserCollectionDataから値を取得
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'            Bson whereFilter = {where_filter};')
                java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('            allFilters.add(combinedFilter);')
            else:
                java_code.append('            allFilters.add(filter);')
            
            java_code.append('        }')
            java_code.append('        Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'        Document doc = collection.find(finalFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
            else:
                java_code.append(f'        Document doc = collection.find(finalFilter).first();')
            
            java_code.append('        if (doc == null) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append(f'         return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, null);')
            java_code.append('    }')
            java_code.append('}')
        else:
            # LIMIT なしの場合、One と Many の両方を生成
            # One バージョンの生成
            java_code.append(f'public static DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData> {method_name}OneWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        List<Bson> allFilters = new ArrayList<>();')
            java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
            java_code.append('            List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
            
            java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            # UserCollectionDataから値を取得
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'            Bson whereFilter = {where_filter};')
                java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('            allFilters.add(combinedFilter);')
            else:
                java_code.append('            allFilters.add(filter);')
            
            java_code.append('        }')
            java_code.append('        Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'        Document doc = collection.find(finalFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
            else:
                java_code.append(f'        Document doc = collection.find(finalFilter).first();')
            
            java_code.append('        if (doc == null) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append(f'         return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, null);')
            java_code.append('    }')
            java_code.append('}')
            
            # Many バージョンの生成
            java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
            java_code.append(f'public static DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>> {method_name}ManyWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    try {')
            java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('        List<Bson> allFilters = new ArrayList<>();')
            java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
            java_code.append('            List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
            
            java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        # UserCollectionDataから値を取得
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            # UserCollectionDataから値を取得
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'            Bson whereFilter = {where_filter};')
                java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('            allFilters.add(combinedFilter);')
            else:
                java_code.append('            allFilters.add(filter);')
            
            java_code.append('        }')
            java_code.append('        Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
            java_code.append('        FindIterable<Document> results = collection.find(finalFilter);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'        results = results.sort(new Document().append({", ".join(sort_fields)}));')
            
            if limit_value:
                java_code.append(f'        results = results.limit({limit_value});')
            
            java_code.append(f'        List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('        for (Document doc : results) {')
            java_code.append(f'            resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        }')
            java_code.append(f'        return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
            java_code.append('    } catch (Exception e) {')
            java_code.append(f'        return DataBaseResultPair.of(false, Collections.emptyList());')
            java_code.append('    }')
            java_code.append('}')

    return java_code

# UserCollectionData クラスの生成
def generate_user_collection_data_class(collection_info):
    java_code = []
    collection = next(iter(collection_info))
    java_code.append('import org.bson.Document;')
    java_code.append('import java.util.List;')
    java_code.append('import java.util.ArrayList;')
    java_code.append('')
    java_code.append(f'public class {collection.capitalize()}CollectionData {{')

    # フィールド定義
    for col in collection_info[collection]["column_list"]:
        java_type = {
            "String": "String",
            "int": "Integer",
            "double": "Double",
            "Date": "Date"
        }.get(col["variable_type"], "Object")
        java_element_type = java_type  # 配列の要素型
        if col.get("is_array", False):
            java_type = f"List<{java_type}>"
        java_code.append(f'    private {java_type} {col["variable_name"]};')
        java_code.append(f'    private boolean {col["variable_name"]}_flag;')

    # デフォルトコンストラクタ
    java_code.append(f'    public {collection.capitalize()}CollectionData() {{')
    for col in collection_info[collection]["column_list"]:
        java_code.append('         @SuppressWarnings("FieldName")')
        java_code.append(f'        this.{col["variable_name"]}_flag = false;')
        if col.get("is_array", False):
            java_element_type = {
                "String": "String",
                "int": "Integer",
                "double": "Double",
                "Date": "Date"
            }.get(col["variable_type"], "Object")
            java_code.append(f'        this.{col["variable_name"]} = new ArrayList<{java_element_type}>();')
    java_code.append('    }')

    # Documentからのコンストラクタ
    java_code.append(f'    public {collection.capitalize()}CollectionData(Document doc) {{')
    for col in collection_info[collection]["column_list"]:
        java_type = col["variable_type"]
        java_element_type = {
            "String": "String",
            "int": "Integer",
            "double": "Double",
            "Date": "Date"
        }.get(java_type, "Object")
        java_code.append(f'        if (doc.containsKey("{col["variable_name"]}")) {{')
        if col.get("is_array", False):
            java_code.append(f'            this.{col["variable_name"]} = doc.getList("{col["variable_name"]}", {java_element_type}.class);')
        else:
            if java_type == "String":
                java_code.append(f'            this.{col["variable_name"]} = doc.getString("{col["variable_name"]}");')
            elif java_type == "int":
                java_code.append(f'            this.{col["variable_name"]} = doc.getInteger("{col["variable_name"]}");')
            elif java_type == "double":
                java_code.append(f'            this.{col["variable_name"]} = doc.getDouble("{col["variable_name"]}");')
            elif java_type == "Date":
                java_code.append(f'            this.{col["variable_name"]} = doc.getDate("{col["variable_name"]}");')
            else:
                java_code.append(f'            this.{col["variable_name"]} = doc.get("{col["variable_name"]}");')
        java_code.append(f'            this.{col["variable_name"]}_flag = true;')
        java_code.append('        } else {')
        if col.get("is_array", False):
            java_code.append(f'            this.{col["variable_name"]} = new ArrayList<{java_element_type}>();')
        java_code.append(f'            this.{col["variable_name"]}_flag = false;')
        java_code.append('        }')
    java_code.append('    }')

    # GetterとSetter
    for col in collection_info[collection]["column_list"]:
        java_type = {
            "String": "String",
            "int": "Integer",
            "double": "Double",
            "Date": "Date"
        }.get(col["variable_type"], "Object")
        java_element_type = java_type
        if col.get("is_array", False):
            java_type = f"List<{java_type}>"
        # Getter
        java_code.append(f'    public {java_type} get{col["variable_name"].capitalize()}() {{')
        java_code.append(f'        return {col["variable_name"]};')
        java_code.append('    }')
        java_code.append(f'    public boolean is{col["variable_name"].capitalize()}Flag() {{')
        java_code.append(f'        return {col["variable_name"]}_flag;')
        java_code.append('    }')
        # Setter
        java_code.append(f'    public void set{col["variable_name"].capitalize()}({java_type} value) {{')
        java_code.append(f'        this.{col["variable_name"]} = value;')
        java_code.append(f'        this.{col["variable_name"]}_flag = true;')
        java_code.append('    }')

    # toDocument メソッド
    java_code.append('    public Document toDocument() {')
    java_code.append('        Document doc = new Document();')
    for col in collection_info[collection]["column_list"]:
        java_code.append(f'        if ({col["variable_name"]}_flag) {{')
        java_code.append(f'            doc.append("{col["variable_name"]}", {col["variable_name"]});')
        java_code.append('        }')
    java_code.append('        return doc;')
    java_code.append('    }')

    java_code.append('}')
    return java_code

# インデックス作成用の Java コードを生成
def generate_index_creation_code(collection_info):
    java_code = []
    collection = next(iter(collection_info))
    java_code.append('    public static void createIndexes(MongoDatabase db) {')
    java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
    
    for col in collection_info[collection]["column_list"]:
        index_type = col.get("index_type", "none")
        if index_type == "hash":
            java_code.append(f'        collection.createIndex(Indexes.hashed("{col["variable_name"]}"));')
        elif index_type == "unique":
            java_code.append(f'        collection.createIndex(Indexes.ascending("{col["variable_name"]}"), new IndexOptions().unique(true));')
        elif index_type == "ascending":
            java_code.append(f'        collection.createIndex(Indexes.ascending("{col["variable_name"]}"));')
        elif index_type == "descending":
            java_code.append(f'        collection.createIndex(Indexes.descending("{col["variable_name"]}"));')
    
    java_code.append('    }')
    return java_code


# 非同期版: 単一引数用の関数
# 非同期版: 単一引数用の関数
def parse_sql_to_mongodb_single_async(sql, method_name, collection_info, auto_index=True):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    
    if operation == "insert":
        fields = re.search(r'\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
        if fields:
            field_list = [f.strip() for f in fields.group(1).split(',')]
            args = [a.strip() for a in fields.group(2).split(',')]
            java_code.append(f'public static CompletableFuture<Boolean> {method_name}Async(MongoDatabase db, {arg_params}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append(f'            {collection.capitalize()}CollectionData data = new {collection.capitalize()}CollectionData();')
            for field, arg in zip(field_list, args):
                java_code.append(f'            data.set{field.capitalize()}({arg.replace("?", "")});')
            java_code.append('            collection.insertOne(data.toDocument());')
            java_code.append('            return true;')
            java_code.append('        } catch (com.mongodb.MongoWriteException e) {')
            java_code.append('            if (e.getCode() == 11000) {')
            java_code.append('                return false;')
            java_code.append('            }')
            java_code.append('            throw e;')
            java_code.append('        } catch (Exception e) {')
            java_code.append('            return false;')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')

    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}Async(MongoDatabase db, {arg_params}) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        if set_clause:
            updates = []
            for set_item in set_clause.group(1).split(','):
                field, value = [x.strip() for x in set_item.split('=')]
                value = clean_value(value)
                
                # フィールド情報を取得
                field_info = next((col for col in collection_info[collection]["column_list"] 
                                 if col["variable_name"] == field), None)
                is_array = field_info.get("is_array", False) if field_info else False
                
                # 配列操作の特別処理
                if is_array and ' - ' in value:
                    # 配列から要素を削除 ($pull)
                    _, element = value.split(' - ')
                    element = element.strip()
                    updates.append(f'Updates.pull("{field}", {element})')
                    continue
                    
                if is_array and ' + ' in value:
                    # 配列に要素を追加 ($push)
                    _, element = value.split(' + ')
                    element = element.strip()
                    updates.append(f'Updates.push("{field}", {element})')
                    continue
                
                # 通常の更新操作
                updates.append(f'Updates.set("{field}", {value.strip()})')
            java_code.append(f'            Bson update = Updates.combine({", ".join(updates)});')

        if where_clause:
            filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index)
            java_code.append(f'            Bson filter = {filters};')
            java_code.append('            UpdateResult result = collection.updateOne(filter, update);')
            java_code.append('            return result.getMatchedCount() > 0;')
        else:
            java_code.append('            UpdateResult result = collection.updateOne(new Document(), update);')
            java_code.append('            return result.getMatchedCount() > 0;')
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}Async(MongoDatabase db, {arg_params}) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        if where_clause:
            filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index)
            java_code.append(f'            Bson filter = {filters};')
            java_code.append('            DeleteResult result = collection.deleteOne(filter);')
            java_code.append('            return result.getDeletedCount() > 0;')
        else:
            java_code.append('            DeleteResult result = collection.deleteOne(new Document());')
            java_code.append('            return result.getDeletedCount() > 0;')
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        return_type = f'DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData>' if limit_value == 1 else f'DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>>'
        return_value = 'DataBaseResultPair.of(false, null)' if limit_value == 1 else 'DataBaseResultPair.of(false, Collections.emptyList())'

        if limit_value == 1:
            java_code.append(f'public static CompletableFuture<{return_type}> {method_name}Async(MongoDatabase db, {arg_params}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            if where_conditions:
                filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                java_code.append(f'            Bson filter = {filters};')
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find(filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'            Document doc = collection.find(filter).first();')
                java_code.append('            if (doc == null) {')
                java_code.append(f'                return {return_value};')
                java_code.append('            }')
                java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find().sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append('            Document doc = collection.find().first();')
                java_code.append('            if (doc == null) {')
                java_code.append(f'                return {return_value};')
                java_code.append('            }')
                java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return {return_value};')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')
        else:
            # One バージョンの生成
            java_code.append(f'public static CompletableFuture<DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData>> {method_name}OneAsync(MongoDatabase db, {arg_params}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            if where_conditions:
                filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                java_code.append(f'            Bson filter = {filters};')
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find(filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'            Document doc = collection.find(filter).first();')
                java_code.append('            if (doc == null) {')
                java_code.append(f'                return DataBaseResultPair.of(false, null);')
                java_code.append('            }')
                java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find().sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append('            Document doc = collection.find().first();')
                java_code.append('            if (doc == null) {')
                java_code.append(f'                return DataBaseResultPair.of(false, null);')
                java_code.append('            }')
                java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')
            
            # Many バージョンの生成
            java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
            java_code.append(f'public static CompletableFuture<DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>>> {method_name}ManyAsync(MongoDatabase db, {arg_params}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            if where_conditions:
                filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                java_code.append(f'            Bson filter = {filters};')
                java_code.append('            FindIterable<Document> results = collection.find(filter);')
            else:
                java_code.append('            FindIterable<Document> results = collection.find();')
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'            results = results.sort(new Document().append({", ".join(sort_fields)}));')
            if limit_value:
                java_code.append(f'            results = results.limit({limit_value});')
            java_code.append(f'            List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('            for (Document doc : results) {')
            java_code.append(f'                resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('            }')
            java_code.append(f'            return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return DataBaseResultPair.of(false, Collections.emptyList());')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')

    return java_code

# 非同期版: UserCollectionData引数用の関数
def parse_sql_to_mongodb_user_collection_data_async(sql, method_name, collection_info, auto_index=True):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    new_params, optional_flag = process_args(sql, arg_params)
    


    
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    
    if operation == "insert":
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}AsyncWithData(MongoDatabase db, {collection.capitalize()}CollectionData data) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('            collection.insertOne(data.toDocument());')
        java_code.append('            return true;')
        java_code.append('        } catch (com.mongodb.MongoWriteException e) {')
        java_code.append('            if (e.getCode() == 11000) {')
        java_code.append('                return false;')
        java_code.append('            }')
        java_code.append('            throw e;')
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}AsyncWithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('            List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            if col["index_type"] != "none":
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
        
        java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        # SET句の解析
        updates = []
        if set_clause:
            for set_item in set_clause.group(1).split(','):
                field, value = [x.strip() for x in set_item.split('=')]
                value = clean_value(value)
                
                # フィールド情報を取得
                field_info = next((col for col in collection_info[collection]["column_list"] 
                                 if col["variable_name"] == field), None)
                is_array = field_info.get("is_array", False) if field_info else False
                
                # 配列操作の特別処理
                if is_array and ' - ' in value:
                    # 配列から要素を削除 ($pull)
                    _, element = value.split(' - ')
                    element = element.strip()
                    updates.append(f'Updates.pull("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                    
                if is_array and ' + ' in value:
                    # 配列に要素を追加 ($push)
                    _, element = value.split(' + ')
                    element = element.strip()
                    updates.append(f'Updates.push("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # 通常の更新操作
                updates.append(f'Updates.set("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
        
        java_code.append(f'            Bson updateOps = Updates.combine({", ".join(updates)});')

        # WHERE句の解析
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    where_filters.append(f'Filters.all("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        value = value.replace("?", "")
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'            Bson whereFilter = {where_filter};')
            java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('            UpdateResult result = collection.updateOne(combinedFilter, updateOps);')
            java_code.append('            return result.getMatchedCount() > 0;')
        else:
            java_code.append('            UpdateResult result = collection.updateOne(filter, updateOps);')
            java_code.append('            return result.getMatchedCount() > 0;')
        
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}AsyncWithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('            List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            if col["index_type"] != "none":
                field = col["variable_name"]
                java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('            }')
        
        java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    where_filters.append(f'Filters.all("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'            Bson whereFilter = {where_filter};')
            java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('            DeleteResult result = collection.deleteOne(combinedFilter);')
            java_code.append('            return result.getDeletedCount() > 0;')
        else:
            java_code.append('            DeleteResult result = collection.deleteOne(filter);')
            java_code.append('            return result.getDeletedCount() > 0;')
        
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        return_type = f'DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData>' if limit_value == 1 else f'DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>>'
        return_value = 'DataBaseResultPair.of(false, null)' if limit_value == 1 else 'DataBaseResultPair.of(false, Collections.emptyList())'

        if limit_value == 1:
            java_code.append(f'public static CompletableFuture<{return_type}> {method_name}AsyncWithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('            List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if col["index_type"] != "none":
                    field = col["variable_name"]
                    java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('            }')
            
            java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'            Bson whereFilter = {where_filter};')
                java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find(combinedFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'            Document doc = collection.find(combinedFilter).first();')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find(filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'            Document doc = collection.find(filter).first();')
            
            java_code.append('            if (doc == null) {')
            java_code.append(f'                return {return_value};')
            java_code.append('            }')
            java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return {return_value};')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')
        else:
            # One バージョンの生成
            java_code.append(f'public static CompletableFuture<DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData>> {method_name}OneAsyncWithData(MongoDatabase db, {collection.capitalize()}CollectionData data{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('            List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if col["index_type"] != "none":
                    field = col["variable_name"]
                    java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('            }')
            
            java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'            Bson whereFilter = {where_filter};')
                java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find(combinedFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'            Document doc = collection.find(combinedFilter).first();')
            else:
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'            Document doc = collection.find(filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'            Document doc = collection.find(filter).first();')
            
            java_code.append('            if (doc == null) {')
            java_code.append(f'                return DataBaseResultPair.of(false, null);')
            java_code.append('            }')
            java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')
            
            # Many バージョンの生成
            java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
            java_code.append(f'public static CompletableFuture<DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>>> {method_name}ManyAsyncWithData(MongoDatabase db, {collection.capitalize()}CollectionData data) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('            List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if col["index_type"] != "none":
                    field = col["variable_name"]
                    java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('            }')
            
            java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        where_filters.append(f'Filters.all("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'            Bson whereFilter = {where_filter};')
                java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('            FindIterable<Document> results = collection.find(combinedFilter);')
            else:
                java_code.append('            FindIterable<Document> results = collection.find(filter);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'            results = results.sort(new Document().append({", ".join(sort_fields)}));')
            
            if limit_value:
                java_code.append(f'            results = results.limit({limit_value});')
            
            java_code.append(f'            List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('            for (Document doc : results) {')
            java_code.append(f'                resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('            }')
            java_code.append(f'            return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return DataBaseResultPair.of(false, Collections.emptyList());')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')

    return java_code

# 非同期版: List<UserCollectionData>引数用の関数
def parse_sql_to_mongodb_list_user_collection_data_async(sql, method_name, collection_info, auto_index=True):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
    limit_value = int(limit_match.group(1)) if limit_match else None
    
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    new_params, optional_flag = process_args(sql, arg_params)

    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    if operation == "insert":
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}AsyncWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('            List<Document> documents = new ArrayList<>();')
        java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
        java_code.append('                documents.add(data.toDocument());')
        java_code.append('            }')
        java_code.append('            collection.insertMany(documents);')
        java_code.append('            return true;')
        java_code.append('        } catch (com.mongodb.MongoWriteException e) {')
        java_code.append('            if (e.getCode() == 11000) {')
        java_code.append('                return false;')
        java_code.append('            }')
        java_code.append('            throw e;')
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}AsyncWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('            List<WriteModel<Document>> updates = new ArrayList<>();')
        java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
        java_code.append('                List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            if col["index_type"] != "none":
                field = col["variable_name"]
                java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
                java_code.append(f'                    filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                java_code.append('                }')
        
        java_code.append('                Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        # SET句の解析
        if set_clause:
            set_updates = []
            for set_item in set_clause.group(1).split(','):
                field, value = [x.strip() for x in set_item.split('=')]
                value = clean_value(value)
                
                # フィールド情報を取得
                field_info = next((col for col in collection_info[collection]["column_list"] 
                                 if col["variable_name"] == field), None)
                is_array = field_info.get("is_array", False) if field_info else False
                
                # 配列操作の特別処理
                if is_array and ' - ' in value:
                    # 配列から要素を削除 ($pull)
                    _, element = value.split(' - ')
                    element = element.strip()
                    set_updates.append(f'Updates.pull("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                    
                if is_array and ' + ' in value:
                    # 配列に要素を追加 ($push)
                    _, element = value.split(' + ')
                    element = element.strip()
                    set_updates.append(f'Updates.push("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # 通常の更新操作
                set_updates.append(f'Updates.set("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
            
            java_code.append(f'                Bson updateOps = Updates.combine({", ".join(set_updates)});')

        # WHERE句の解析
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    where_filters.append(f'Filters.all("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    where_filters.append(f'Filters.in("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'                Bson whereFilter = {where_filter};')
            java_code.append('                Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('                updates.add(new UpdateManyModel<>(combinedFilter, updateOps));')
        else:
            java_code.append('                updates.add(new UpdateManyModel<>(filter, updateOps));')
        
        java_code.append('            }')
        java_code.append('            if (!updates.isEmpty()) {')
        java_code.append('                BulkWriteResult result = collection.bulkWrite(updates);')
        java_code.append('                return result.getModifiedCount() > 0;')
        java_code.append('            }')
        java_code.append('            return false;')
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        java_code.append(f'public static CompletableFuture<Boolean> {method_name}AsyncWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
        java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
        java_code.append('            List<WriteModel<Document>> deletes = new ArrayList<>();')
        java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
        java_code.append('                List<Bson> filters = new ArrayList<>();')
        
        # UserCollectionDataからフィルターを生成
        for col in collection_info[collection]["column_list"]:
            field = col["variable_name"]
            java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
            java_code.append(f'                    filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
            java_code.append('                }')
        
        java_code.append('                Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
        
        if where_clause:
            where_filters = []
            where_conditions = where_clause.group(1)
            
            # 条件を分割
            for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                condition = condition.strip()
                
                # ALL演算子の処理
                if ' ALL ' in condition:
                    field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                    where_filters.append(f'Filters.all("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # IN演算子の処理
                if ' IN ' in condition.upper():
                    field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                    where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                    continue
                
                # その他の比較演算子
                for op in comparison_operators.keys():
                    if op in condition:
                        parts = condition.split(op, 1)
                        if len(parts) < 2:
                            continue
                        
                        field, value = [clean_value(x) for x in parts]
                        where_filters.append(f'Filters.{comparison_operators[op]}("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        break
            
            # インデックスの自動追加
            if auto_index:
                for col in collection_info[collection]["column_list"]:
                    if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                        field = col["variable_name"]
                        if not any(f'"{field}"' in f for f in where_filters):
                            where_filters.append(f'Filters.exists("{field}")')
            
            # フィルターを結合
            if where_filters:
                where_filter = f'Filters.and({", ".join(where_filters)})'
            else:
                where_filter = 'new Document()'
            
            java_code.append(f'                Bson whereFilter = {where_filter};')
            java_code.append('                Bson combinedFilter = Filters.and(filter, whereFilter);')
            java_code.append('                deletes.add(new DeleteManyModel<>(combinedFilter));')
        else:
            java_code.append('                deletes.add(new DeleteManyModel<>(filter));')
        
        java_code.append('            }')
        java_code.append('            if (!deletes.isEmpty()) {')
        java_code.append('                BulkWriteResult result = collection.bulkWrite(deletes);')
        java_code.append('                return result.getDeletedCount() > 0;')
        java_code.append('            }')
        java_code.append('            return false;')
        java_code.append('        } catch (Exception e) {')
        java_code.append('            return false;')
        java_code.append('        }')
        java_code.append('    });')
        java_code.append('}')

    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        return_type = f'DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData>' if limit_value == 1 else f'DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>>'
        return_value = 'DataBaseResultPair.of(false, null)' if limit_value == 1 else 'DataBaseResultPair.of(false, Collections.emptyList())'

        if limit_value == 1:
            java_code.append(f'public static CompletableFuture<{return_type}> {method_name}AsyncWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('            List<Bson> allFilters = new ArrayList<>();')
            java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
            java_code.append('                List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if col["index_type"] != "none":
                    field = col["variable_name"]
                    java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                    filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('                }')
            
            java_code.append('                Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        where_filters.append(f'Filters.all("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        where_filters.append(f'Filters.in("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'                Bson whereFilter = {where_filter};')
                java_code.append('                Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('                allFilters.add(combinedFilter);')
            else:
                java_code.append('                allFilters.add(filter);')
            
            java_code.append('            }')
            java_code.append('            Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'            Document doc = collection.find(finalFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
            else:
                java_code.append(f'            Document doc = collection.find(finalFilter).first();')
            
            java_code.append('            if (doc == null) {')
            java_code.append(f'                return {return_value};')
            java_code.append('            }')
            java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return {return_value};')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')
        else:
            # One バージョンの生成
            java_code.append(f'public static CompletableFuture<DataBaseResultPair<Boolean, {collection.capitalize()}CollectionData>> {method_name}OneAsyncWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('            List<Bson> allFilters = new ArrayList<>();')
            java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
            java_code.append('                List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if col["index_type"] != "none":
                    field = col["variable_name"]
                    java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                    filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('                }')
            
            java_code.append('                Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        where_filters.append(f'Filters.all("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        where_filters.append(f'Filters.in("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'                Bson whereFilter = {where_filter};')
                java_code.append('                Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('                allFilters.add(combinedFilter);')
            else:
                java_code.append('                allFilters.add(filter);')
            
            java_code.append('            }')
            java_code.append('            Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'            Document doc = collection.find(finalFilter).sort(new Document().append({", ".join(sort_fields)})).first();')
            else:
                java_code.append(f'            Document doc = collection.find(finalFilter).first();')
            
            java_code.append('            if (doc == null) {')
            java_code.append(f'                return DataBaseResultPair.of(false, null);')
            java_code.append('            }')
            java_code.append(f'            return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')
            
            # Many バージョンの生成
            java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
            java_code.append(f'public static CompletableFuture<DataBaseResultPair<Boolean, List<{collection.capitalize()}CollectionData>>> {method_name}ManyAsyncWithDataList(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList{", " + new_params if optional_flag else ""}) {{')
            java_code.append('    return CompletableFuture.supplyAsync(() -> {')
            java_code.append('        try {')
            java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
            java_code.append('            List<Bson> allFilters = new ArrayList<>();')
            java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
            java_code.append('                List<Bson> filters = new ArrayList<>();')
            
            # UserCollectionDataからフィルターを生成
            for col in collection_info[collection]["column_list"]:
                if col["index_type"] != "none":
                    field = col["variable_name"]
                    java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
                    java_code.append(f'                    filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                    java_code.append('                }')
            
            java_code.append('                Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
            
            if where_conditions:
                where_filters = []
                
                # 条件を分割
                for condition in re.split(r'\bAND\b|\bOR\b', where_conditions, flags=re.IGNORECASE):
                    condition = condition.strip()
                    
                    # ALL演算子の処理
                    if ' ALL ' in condition:
                        field, value = [clean_value(x) for x in condition.split(' ALL ', 1)]
                        where_filters.append(f'Filters.all("{field}",  {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # IN演算子の処理
                    if ' IN ' in condition.upper():
                        field, value = [clean_value(x) for x in condition.split(' IN ', 1)]
                        where_filters.append(f'Filters.in("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                        continue
                    
                    # その他の比較演算子
                    for op in comparison_operators.keys():
                        if op in condition:
                            parts = condition.split(op, 1)
                            if len(parts) < 2:
                                continue
                            
                            field, value = [clean_value(x) for x in parts]
                            where_filters.append(f'Filters.{comparison_operators[op]}("{field}", {value if optional_flag and  is_optional_arg_present(new_params,value) else  "data.get" + field.capitalize() + "()"})')
                            break
                
                # インデックスの自動追加
                if auto_index:
                    for col in collection_info[collection]["column_list"]:
                        if col.get("index_type", "none") in ["ascending", "descending", "hash", "unique"]:
                            field = col["variable_name"]
                            if not any(f'"{field}"' in f for f in where_filters):
                                where_filters.append(f'Filters.exists("{field}")')
                
                # フィルターを結合
                if where_filters:
                    where_filter = f'Filters.and({", ".join(where_filters)})'
                else:
                    where_filter = 'new Document()'
                
                java_code.append(f'                Bson whereFilter = {where_filter};')
                java_code.append('                Bson combinedFilter = Filters.and(filter, whereFilter);')
                java_code.append('                allFilters.add(combinedFilter);')
            else:
                java_code.append('                allFilters.add(filter);')
            
            java_code.append('            }')
            java_code.append('            Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
            java_code.append('            FindIterable<Document> results = collection.find(finalFilter);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'            results = results.sort(new Document().append({", ".join(sort_fields)}));')
            
            if limit_value:
                java_code.append(f'            results = results.limit({limit_value});')
            
            java_code.append(f'            List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('            for (Document doc : results) {')
            java_code.append(f'                resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('            }')
            java_code.append(f'            return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
            java_code.append('        } catch (Exception e) {')
            java_code.append(f'            return DataBaseResultPair.of(false, Collections.emptyList());')
            java_code.append('        }')
            java_code.append('    });')
            java_code.append('}')

    return java_code

# 非同期版: バルク操作
def generate_bulk_operations_async(collection_info):
    java_code = []
    collection = next(iter(collection_info))
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    java_code.append(f'public static CompletableFuture<Boolean> bulkInsert{collection.capitalize()}Async(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList) {{')
    java_code.append('    return CompletableFuture.supplyAsync(() -> {')
    java_code.append('        try {')
    java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
    java_code.append('            List<Document> documents = new ArrayList<>();')
    java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
    java_code.append('                documents.add(data.toDocument());')
    java_code.append('            }')
    java_code.append('            collection.insertMany(documents);')
    java_code.append('            return true;')
    java_code.append('        } catch (com.mongodb.MongoWriteException e) {')
    java_code.append('            if (e.getCode() == 11000) {')
    java_code.append('                return false;')
    java_code.append('            }')
    java_code.append('            throw e;')
    java_code.append('        } catch (Exception e) {')
    java_code.append('            return false;')
    java_code.append('        }')
    java_code.append('    });')
    java_code.append('}')

    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    java_code.append(f'public static CompletableFuture<Boolean> bulkUpdate{collection.capitalize()}Async(MongoDatabase db, List<{collection.capitalize()}CollectionData> dataList) {{')
    java_code.append('    return CompletableFuture.supplyAsync(() -> {')
    java_code.append('        try {')
    java_code.append(f'            MongoCollection<Document> collection = db.getCollection("{collection}");')
    java_code.append('            List<WriteModel<Document>> updates = new ArrayList<>();')
    java_code.append(f'            for ({collection.capitalize()}CollectionData data : dataList) {{')
    java_code.append('                List<Bson> filters = new ArrayList<>();')
    for col in collection_info[collection]["column_list"]:
        field = col["variable_name"]
        java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
        java_code.append(f'                    filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
        java_code.append('                }')
    java_code.append('                Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
    java_code.append('                List<Bson> updateOps = new ArrayList<>();')
    for col in collection_info[collection]["column_list"]:
        field = col["variable_name"]
        java_code.append(f'                if (data.is{field.capitalize()}Flag()) {{')
        java_code.append(f'                    updateOps.add(Updates.set("{field}", data.get{field.capitalize()}()));')
        java_code.append('                }')
    java_code.append('                if (!updateOps.isEmpty()) {')
    java_code.append('                    updates.add(new UpdateManyModel<>(filter, Updates.combine(updateOps)));')
    java_code.append('                }')
    java_code.append('            }')
    java_code.append('            if (!updates.isEmpty()) {')
    java_code.append('                BulkWriteResult result = collection.bulkWrite(updates);')
    java_code.append('                return result.getModifiedCount() > 0;')
    java_code.append('            }')
    java_code.append('            return false;')
    java_code.append('        } catch (Exception e) {')
    java_code.append('            return false;')
    java_code.append('        }')
    java_code.append('    });')
    java_code.append('}')

    return java_code

def parse_sql_to_mongodb_transaction(sql, method_name, collection_info, auto_index=True, is_async=False, is_with_data=False, is_list=False):
    parsed = sqlparse.parse(sql)[0]
    operation = parsed.get_type().lower()
    java_code = []

    table_match = re.search(r'\bFROM\s+(\w+)|INTO\s+(\w+)', sql, re.IGNORECASE)
    collection = table_match.group(1) or table_match.group(2) if table_match else next(iter(collection_info))
    
    args = sorted(set(re.findall(r'arg\d+', sql)), key=lambda x: int(x[3:]))
    arg_params = ', '.join(f'{get_arg_type(collection, arg, sql, collection_info)} {arg}' for arg in args)
    new_params, optional_flag = process_args(sql, arg_params) if is_with_data or is_list else (arg_params, False)
    
    # データ引数の設定
    if is_with_data:
        if is_list:
            data_param = f'List<{collection.capitalize()}CollectionData> dataList'
        else:
            data_param = f'{collection.capitalize()}CollectionData data'
    else:
        data_param = ''
    
    # トランザクション用のセッションパラメータ
    session_param = 'ClientSession session'
    
    # 戻り値の型を同期/非同期で切り替え
    if is_async:
        return_type = 'CompletableFuture<Boolean>' if operation in ["insert", "update", "delete"] else 'CompletableFuture<DataBaseResultPair<Boolean, %s>>'
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        if operation == "select":
            if limit_value == 1:
                return_type = return_type % f'{collection.capitalize()}CollectionData'
            else:
                return_type = return_type % f'List<{collection.capitalize()}CollectionData>'
        else:
            return_type = 'CompletableFuture<Boolean>'
    else:
        return_type = 'boolean' if operation in ["insert", "update", "delete"] else 'DataBaseResultPair<Boolean, %s>'
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        if operation == "select":
            if limit_value == 1:
                return_type = return_type % f'{collection.capitalize()}CollectionData'
            else:
                return_type = return_type % f'List<{collection.capitalize()}CollectionData>'
        else:
            return_type = 'boolean'
    
    # メソッドシグネチャの生成
    method_signature = f'public static {return_type} {method_name}{"Async" if is_async else ""}(MongoDatabase db, {session_param}, {data_param}{", " + new_params if (is_with_data or is_list) and new_params else ""}) {{'
    
    java_code.append('@SuppressWarnings({"java:S3776", "unused"})')
    java_code.append(method_signature)
    
    # 非同期の場合は CompletableFuture.supplyAsync を使用
    if is_async:
        java_code.append('    return CompletableFuture.supplyAsync(() -> {')
        java_code.append('        try {')
    else:
        java_code.append('    try {')
    
    java_code.append(f'        MongoCollection<Document> collection = db.getCollection("{collection}");')
    
    if operation == "insert":
        if is_with_data:
            if is_list:
                java_code.append('        List<Document> documents = new ArrayList<>();')
                java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
                java_code.append('            documents.add(data.toDocument());')
                java_code.append('        }')
                java_code.append('        collection.insertMany(session, documents);')
            else:
                java_code.append('        collection.insertOne(session, data.toDocument());')
        else:
            fields = re.search(r'\((.*?)\)\s*VALUES\s*\((.*?)\)', sql, re.IGNORECASE)
            if fields:
                field_list = [f.strip() for f in fields.group(1).split(',')]
                args = [a.strip() for a in fields.group(2).split(',')]
                java_code.append(f'        {collection.capitalize()}CollectionData data = new {collection.capitalize()}CollectionData();')
                for field, arg in zip(field_list, args):
                    java_code.append(f'        data.set{field.capitalize()}({arg.replace("?", "")});')
                java_code.append('        collection.insertOne(session, data.toDocument());')
        java_code.append('        return true;')
    
    elif operation == "update":
        set_clause = re.search(r'SET\s+(.*?)\s*WHERE', sql, re.IGNORECASE)
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        
        if is_with_data:
            if is_list:
                java_code.append('        List<WriteModel<Document>> updates = new ArrayList<>();')
                java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
                java_code.append('            List<Bson> filters = new ArrayList<>();')
                for col in collection_info[collection]["column_list"]:
                    if col["index_type"] != "none":
                        field = col["variable_name"]
                        java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                        java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                        java_code.append('            }')
                java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                
                updates = []
                if set_clause:
                    for set_item in set_clause.group(1).split(','):
                        field, value = [x.strip() for x in set_item.split('=')]
                        value = clean_value(value)
                        updates.append(f'Updates.set("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                java_code.append(f'            Bson updateOps = Updates.combine({", ".join(updates)});')
                
                if where_clause:
                    filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index, is_with_data=True)
                    java_code.append(f'            Bson whereFilter = {filters};')
                    java_code.append('            Bson combinedFilter = Filters.and(filter, FeesFilter);')
                    java_code.append('            updates.add(new UpdateManyModel<>(combinedFilter, updateOps));')
                else:
                    java_code.append('            updates.add(new UpdateManyModel<>(filter, updateOps));')
                java_code.append('        }')
                java_code.append('        ifい        if (!updates.isEmpty()) {')
                java_code.append('            BulkWriteResult result = collection.bulkWrite(session, updates);')
                java_code.append('            return result.getModifiedCount() > 0;')
                java_code.append('        }')
                java_code.append('        return false;')
            else:
                java_code.append('        List<Bson> filters = new ArrayList<>();')
                for col in collection_info[collection]["column_list"]:
                    if col["index_type"] != "none":
                        field = col["variable_name"]
                        java_code.append(f'        if (data.is{field.capitalize()}Flag()) {{')
                        java_code.append(f'            filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                        java_code.append('        }')
                java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                
                updates = []
                if set_clause:
                    for set_item in set_clause.group(1).split(','):
                        field, value = [x.strip() for x in set_item.split('=')]
                        value = clean_value(value)
                        updates.append(f'Updates.set("{field}", {value if optional_flag and is_optional_arg_present(new_params, value) else "data.get" + field.capitalize() + "()"})')
                java_code.append(f'        Bson updateOps = Updates.combine({", ".join(updates)});')
                
                if where_clause:
                    filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index, is_with_data=True)
                    java_code.append(f'        Bson whereFilter = {filters};')
                    java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                    java_code.append('        UpdateResult result = collection.updateOne(session, combinedFilter, updateOps);')
                else:
                    java_code.append('        UpdateResult result = collection.updateOne(session, filter, updateOps);')
                java_code.append('        return result.getMatchedCount() > 0;')
        else:
            if set_clause:
                updates = []
                for set_item in set_clause.group(1).split(','):
                    field, value = [x.strip() for x in set_item.split('=')]
                    value = clean_value(value)
                    updates.append(f'Updates.set("{field}", {value})')
                java_code.append(f'        Bson update = Updates.combine({", ".join(updates)});')
            
            if where_clause:
                filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index)
                java_code.append(f'        Bson filter = {filters};')
                java_code.append('        UpdateResult result = collection.updateOne(session, filter, update);')
            else:
                java_code.append('        UpdateResult result = collection.updateOne(session, new Document(), update);')
            java_code.append('        return result.getMatchedCount() > 0;')
    
    elif operation == "delete":
        where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE)
        
        if is_with_data:
            if is_list:
                java_code.append('        List<WriteModel<Document>> deletes = new ArrayList<>();')
                java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
                java_code.append('            List<Bson> filters = new ArrayList<>();')
                for col in collection_info[collection]["column_list"]:
                    if col["index_type"] != "none":
                        field = col["variable_name"]
                        java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                        java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                        java_code.append('            }')
                java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                
                if where_clause:
                    filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index, is_with_data=True)
                    java_code.append(f'            Bson whereFilter = {filters};')
                    java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                    java_code.append('            deletes.add(new DeleteManyModel<>(combinedFilter));')
                else:
                    java_code.append('            deletes.add(new DeleteManyModel<>(filter));')
                java_code.append('        }')
                java_code.append('        if (!deletes.isEmpty()) {')
                java_code.append('            BulkWriteResult result = collection.bulkWrite(session, deletes);')
                java_code.append('            return result.getDeletedCount() > 0;')
                java_code.append('        }')
                java_code.append('        return false;')
            else:
                java_code.append('        List<Bson> filters = new ArrayList<>();')
                for col in collection_info[collection]["column_list"]:
                    if col["index_type"] != "none":
                        field = col["variable_name"]
                        java_code.append(f'        if (data.is{field.capitalize()}Flag()) {{')
                        java_code.append(f'            filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                        java_code.append('        }')
                java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                
                if where_clause:
                    filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index, is_with_data=True)
                    java_code.append(f'        Bson whereFilter = {filters};')
                    java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                    java_code.append('        DeleteResult result = collection.deleteOne(session, combinedFilter);')
                else:
                    java_code.append('        DeleteResult result = collection.deleteOne(session, filter);')
                java_code.append('        return result.getDeletedCount() > 0;')
        else:
            if where_clause:
                filters = parse_where_clause(where_clause.group(1), collection_info, collection, auto_index=auto_index)
                java_code.append(f'        Bson filter = {filters};')
                java_code.append('        DeleteResult result = collection.deleteOne(session, filter);')
            else:
                java_code.append('        DeleteResult result = collection.deleteOne(session, new Document());')
            java_code.append('        return result.getDeletedCount() > 0;')
    
    elif operation == "select":
        where_clause = re.search(r'WHERE\s+(.*?)(?:\s*(?:ORDER\s+BY\s+(.*?)|LIMIT\s+\d+))?$', sql, re.IGNORECASE)
        order_by_clause = where_clause.group(2) if where_clause and where_clause.group(2) else None
        where_conditions = where_clause.group(1) if where_clause else None
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        limit_value = int(limit_match.group(1)) if limit_match else None
        
        if limit_value == 1:
            if is_with_data:
                if is_list:
                    java_code.append('        List<Bson> allFilters = new ArrayList<>();')
                    java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
                    java_code.append('            List<Bson> filters = new ArrayList<>();')
                    for col in collection_info[collection]["column_list"]:
                        if col["index_type"] != "none":
                            field = col["variable_name"]
                            java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                            java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                            java_code.append('            }')
                    java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                    
                    if where_conditions:
                        filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index, is_with_data=True)
                        java_code.append(f'            Bson whereFilter = {filters};')
                        java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                        java_code.append('            allFilters.add(combinedFilter);')
                    else:
                        java_code.append('            allFilters.add(filter);')
                    java_code.append('        }')
                    java_code.append('        Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
                else:
                    java_code.append('        List<Bson> filters = new ArrayList<>();')
                    for col in collection_info[collection]["column_list"]:
                        if col["index_type"] != "none":
                            field = col["variable_name"]
                            java_code.append(f'        if (data.is{field.capitalize()}Flag()) {{')
                            java_code.append(f'            filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                            java_code.append('        }')
                    java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                    
                    if where_conditions:
                        filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index, is_with_data=True)
                        java_code.append(f'        Bson whereFilter = {filters};')
                        java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                
                if order_by_clause:
                    sort_fields = []
                    for sort_item in order_by_clause.split(','):
                        field, *direction = sort_item.strip().split()
                        direction = direction[0].upper() if direction else 'ASC'
                        sort_value = '1' if direction == 'ASC' else '-1'
                        sort_fields.append(f'"{field}", {sort_value}')
                    java_code.append(f'        Document doc = collection.find(session, {"finalFilter" if is_list else "combinedFilter"}).sort(new Document().append({", ".join(sort_fields)})).first();')
                else:
                    java_code.append(f'        Document doc = collection.find(session, {"finalFilter" if is_list else "combinedFilter"}).first();')
            else:
                if where_conditions:
                    filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                    java_code.append(f'        Bson filter = {filters};')
                    if order_by_clause:
                        sort_fields = []
                        for sort_item in order_by_clause.split(','):
                            field, *direction = sort_item.strip().split()
                            direction = direction[0].upper() if direction else 'ASC'
                            sort_value = '1' if direction == 'ASC' else '-1'
                            sort_fields.append(f'"{field}", {sort_value}')
                        java_code.append(f'        Document doc = collection.find(session, filter).sort(new Document().append({", ".join(sort_fields)})).first();')
                    else:
                        java_code.append(f'        Document doc = collection.find(session, filter).first();')
                else:
                    if order_by_clause:
                        sort_fields = []
                        for sort_item in order_by_clause.split(','):
                            field, *direction = sort_item.strip().split()
                            direction = direction[0].upper() if direction else 'ASC'
                            sort_value = '1' if direction == 'ASC' else '-1'
                            sort_fields.append(f'"{field}", {sort_value}')
                        java_code.append(f'        Document doc = collection.find(session).sort(new Document().append({", ".join(sort_fields)})).first();')
                    else:
                        java_code.append('        Document doc = collection.find(session).first();')
            
            java_code.append('        if (doc == null) {')
            java_code.append('            return DataBaseResultPair.of(false, null);')
            java_code.append('        }')
            java_code.append(f'        return DataBaseResultPair.of(true, new {collection.capitalize()}CollectionData(doc));')
        else:
            if is_with_data:
                if is_list:
                    java_code.append('        List<Bson> allFilters = new ArrayList<>();')
                    java_code.append(f'        for ({collection.capitalize()}CollectionData data : dataList) {{')
                    java_code.append('            List<Bson> filters = new ArrayList<>();')
                    for col in collection_info[collection]["column_list"]:
                        if col["index_type"] != "none":
                            field = col["variable_name"]
                            java_code.append(f'            if (data.is{field.capitalize()}Flag()) {{')
                            java_code.append(f'                filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                            java_code.append('            }')
                    java_code.append('            Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                    
                    if where_conditions:
                        filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index, is_with_data=True)
                        java_code.append(f'            Bson whereFilter = {filters};')
                        java_code.append('            Bson combinedFilter = Filters.and(filter, whereFilter);')
                        java_code.append('            allFilters.add(combinedFilter);')
                    else:
                        java_code.append('            allFilters.add(filter);')
                    java_code.append('        }')
                    java_code.append('        Bson finalFilter = allFilters.isEmpty() ? new Document() : Filters.or(allFilters);')
                    java_code.append('        FindIterable<Document> results = collection.find(session, finalFilter);')
                else:
                    java_code.append('        List<Bson> filters = new ArrayList<>();')
                    for col in collection_info[collection]["column_list"]:
                        if col["index_type"] != "none":
                            field = col["variable_name"]
                            java_code.append(f'        if (data.is{field.capitalize()}Flag()) {{')
                            java_code.append(f'            filters.add(Filters.eq("{field}", data.get{field.capitalize()}()));')
                            java_code.append('        }')
                    java_code.append('        Bson filter = filters.isEmpty() ? new Document() : Filters.and(filters);')
                    
                    if where_conditions:
                        filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index, is_with_data=True)
                        java_code.append(f'        Bson whereFilter = {filters};')
                        java_code.append('        Bson combinedFilter = Filters.and(filter, whereFilter);')
                        java_code.append('        FindIterable<Document> results = collection.find(session, combinedFilter);')
                    else:
                        java_code.append('        FindIterable<Document> results = collection.find(session, filter);')
            else:
                if where_conditions:
                    filters = parse_where_clause(where_conditions, collection_info, collection, auto_index=auto_index)
                    java_code.append(f'        Bson filter = {filters};')
                    java_code.append('        FindIterable<Document> results = collection.find(session, filter);')
                else:
                    java_code.append('        FindIterable<Document> results = collection.find(session);')
            
            if order_by_clause:
                sort_fields = []
                for sort_item in order_by_clause.split(','):
                    field, *direction = sort_item.strip().split()
                    direction = direction[0].upper() if direction else 'ASC'
                    sort_value = '1' if direction == 'ASC' else '-1'
                    sort_fields.append(f'"{field}", {sort_value}')
                java_code.append(f'        results = results.sort(new Document().append({", ".join(sort_fields)}));')
            
            if limit_value:
                java_code.append(f'        results = results.limit({limit_value});')
            
            java_code.append(f'        List<{collection.capitalize()}CollectionData> resultList = new ArrayList<>();')
            java_code.append('        for (Document doc : results) {')
            java_code.append(f'            resultList.add(new {collection.capitalize()}CollectionData(doc));')
            java_code.append('        }')
            java_code.append('        return resultList.isEmpty() ? DataBaseResultPair.of(false, Collections.emptyList()) : DataBaseResultPair.of(true, resultList);')
    
    # 例外処理と関数の終了
    java_code.append('        } catch (Exception e) {')
    if operation == "select" and limit_value != 1:
        java_code.append('            return DataBaseResultPair.of(false, Collections.emptyList());')
    elif operation == "select":
        java_code.append('            return DataBaseResultPair.of(false, null);')
    else:
        java_code.append('            return false;')
    java_code.append('        }')
    
    if is_async:
        java_code.append('    });')
    
    java_code.append('}')
    
    return java_code

# writeJavaCode関数内のSQLクエリごとのコード生成部分に以下を追加
def writeJavaCode(collection, write_path,parent_path="io.github.chigadio.javamongodbbridge"):
    os.makedirs(write_path, exist_ok=True)
    catitalize_data = next(iter(collection_info))
    
    with open(write_path + f"/{catitalize_data.capitalize()}CollectionData.java", mode="w", encoding="utf-8") as f:
        for line in generate_user_collection_data_class(collection_info):
            f.write(line + '\n')
        
    with open(write_path+ f"/{catitalize_data.capitalize()}Repository.java", mode="w", encoding="utf-8") as f:
        # インポート文（修正済み、CompletableFutureを追加）
        f.write('import com.mongodb.client.MongoCollection;\n')
        f.write('import com.mongodb.client.MongoDatabase;\n')
        f.write('import com.mongodb.client.FindIterable;\n')
        f.write('import com.mongodb.client.model.Filters;\n')
        f.write('import com.mongodb.client.model.Updates;\n')
        f.write('import com.mongodb.client.model.Indexes;\n')
        f.write('import com.mongodb.client.model.IndexOptions;\n')
        f.write('import com.mongodb.client.model.WriteModel;\n')
        f.write('import com.mongodb.client.model.UpdateManyModel;\n')
        f.write('import com.mongodb.client.model.DeleteManyModel;\n')
        f.write('import com.mongodb.client.result.UpdateResult;\n')
        f.write('import com.mongodb.client.result.DeleteResult;\n')
        f.write('import com.mongodb.bulk.BulkWriteResult;\n')
        f.write('import org.bson.Document;\n')
        f.write('import org.bson.conversions.Bson;\n')
        f.write('import java.util.ArrayList;\n')
        f.write('import java.util.List;\n')
        f.write('import java.util.Collections;\n')
        f.write('import java.util.concurrent.CompletableFuture;\n')  # 追加
        f.write(f'import {parent_path}.util.variable.DataBaseResultPair;')
        f.write('\n')
        
        
        f.write('\n')
        
        # インデックス作成クラスの生成
        for line in generate_index_creation_code(collection_info):
            f.write(line + '\n')
        
        f.write('\n')
        
        # バルク操作の生成（同期版）
        for line in generate_bulk_operations(collection_info):
            f.write("   " + line + '\n')
        
        f.write('\n')
        
        # バルク操作の生成（非同期版）
        f.write('// Asynchronous Bulk Operations\n')
        for line in generate_bulk_operations_async(collection_info):
            f.write("   " + line + '\n')
        
        f.write('\n')
        
        # SQLクエリでコード生成
        for item in collection_info["users"]["queries"]:
            sql = item["query"]
            method_name = item["method_name"]
            # 同期版（既存）
            f.write(f'// SQL: {sql}\n')
            f.write(f'// Generated Java MongoDB Code for method: {method_name} (Single Arguments)\n')
            for line in parse_sql_to_mongodb_single(sql, method_name, collection_info, auto_index=True,is_transaction=True):

                f.write("   " + line + '\n')
            f.write('\n')
            f.write(f'// Generated Java MongoDB Code for method: {method_name}NoAutoIndex (Single Arguments)\n')
            for line in parse_sql_to_mongodb_single(sql, f'{method_name}NoAutoIndex', collection_info, auto_index=False,is_transaction=True):
#
                f.write("   " + line + '\n')
            f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}WithData (UsersCollectionData Argument)\n')
            #for line in parse_sql_to_mongodb_user_collection_data(sql, method_name, collection_info, auto_index=True):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}NoAutoIndexWithData (UsersCollectionData Argument)\n')
            #for line in parse_sql_to_mongodb_user_collection_data(sql, f'{method_name}NoAutoIndex', collection_info, auto_index=False):
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}WithDataList (List<UsersCollectionData> Argument)\n')
            #for line in parse_sql_to_mongodb_list_user_collection_data(sql, method_name, collection_info, auto_index=True):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}NoAutoIndexWithDataList (List<UsersCollectionData> Argument)\n')
            #for line in parse_sql_to_mongodb_list_user_collection_data(sql, f'{method_name}NoAutoIndex', collection_info, auto_index=False):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            ##
            ### 非同期版
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}Async (Single Arguments, Async)\n')
            #for line in parse_sql_to_mongodb_single_async(sql, method_name, collection_info, auto_index=True):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}NoAutoIndexAsync (Single Arguments, Async)\n')
            #for line in parse_sql_to_mongodb_single_async(sql, f'{method_name}NoAutoIndex', collection_info, auto_index=False):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}AsyncWithData (UsersCollectionData Argument, Async)\n')
            #for line in parse_sql_to_mongodb_user_collection_data_async(sql, method_name, collection_info, auto_index=True):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}NoAutoIndexAsyncWithData (UsersCollectionData Argument, Async)\n')
            #for line in parse_sql_to_mongodb_user_collection_data_async(sql, f'{method_name}NoAutoIndex', collection_info, auto_index=False):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}AsyncWithDataList (List<UsersCollectionData> Argument, Async)\n')
            #for line in parse_sql_to_mongodb_list_user_collection_data_async(sql, method_name, collection_info, auto_index=True):
##
            #    f.write("   " + line + '\n')
            #f.write('\n')
            #f.write(f'// Generated Java MongoDB Code for method: {method_name}NoAutoIndexAsyncWithDataList (List<UsersCollectionData> Argument, Async)\n')
            #for line in parse_sql_to_mongodb_list_user_collection_data_async(sql, f'{method_name}NoAutoIndex', collection_info, auto_index=False):
            #    f.write("   " + line + '\n')
            #f.write('\n')
            

writeJavaCode(collection=collection_info,write_path="./Java/Generate/Users")

