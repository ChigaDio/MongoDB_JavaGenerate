import json

def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def generate_class_name(type_name):
    components = type_name.split('_')
    camel_case = ''.join(x[0].upper() + x[1:] for x in components)
    return f"{camel_case}CollectionData"

def generate_java_type(var_type):
    type_map = {"string": "String", "int": "Integer", "bool": "Boolean", "double": "Double"}
    return type_map.get(var_type, var_type)

def generate_field_declaration(column):
    var_type = generate_java_type(column["variable_type"])
    var_name = column["variable_name"]
    if column["is_array"]:
        var_type = f"List<{var_type}>"
    return f"    private {var_type} {var_name}; // {column['variable_explanation']}"

def generate_getter(column):
    var_type = generate_java_type(column["variable_type"])
    var_name = column["variable_name"]
    camel_name = to_camel_case(var_name)
    if column["is_array"]:
        var_type = f"List<{var_type}>"
    return f"""
    public {var_type} get{camel_name[0].upper() + camel_name[1:]}() {{
        return {var_name};
    }}"""

def generate_setter(column):
    var_type = generate_java_type(column["variable_type"])
    var_name = column["variable_name"]
    camel_name = to_camel_case(var_name)
    if column["is_array"]:
        var_type = f"List<{var_type}>"
    return f"""
    public void set{camel_name[0].upper() + camel_name[1:]}({var_type} {var_name}) {{
        this.{var_name} = {var_name};
    }}"""

def get_getter_name(field, prefix="item"):
    if "." in field:
        parts = field.split(".")
        result = prefix
        for part in parts:
            camel_part = to_camel_case(part)
            result += f".get{camel_part[0].upper() + camel_part[1:]}()"
        return result
    camel_field = to_camel_case(field)
    return f"{prefix}.get{camel_field[0].upper() + camel_field[1:]}()"

def generate_cache_key_getter(columns, index_fields):
    key_getters = []
    for field in index_fields:
        key_getters.append(f"{get_getter_name(field, 'data')}")
    return key_getters

def get_index_types(columns, index_fields):
    index_types = []
    for field in index_fields:
        for col in columns:
            if col["variable_name"] == field:
                index_types.append(generate_java_type(col["variable_type"]))
                break
    return index_types



def generate_query_methods(query, collection_name, class_name, columns):
    method_name = query["method_name"]
    where_conditions = query.get("where", [])
    query_type = query["type"]
    methods = []

    return_type_single = f"DataBaseResultPair<Boolean, {class_name}>"
    return_type_many = f"DataBaseResultPair<Boolean, List<{class_name}>>"

    # インデックスフィールドを取得（優先順位: unique > hash > index）
    index_fields = []
    for col in columns:
        if col["index_type"] == "unique":
            index_fields.insert(0, col["variable_name"])
        elif col["index_type"] == "hash":
            index_fields.append(col["variable_name"])
        elif col["index_type"] == "index":
            index_fields.append(col["variable_name"])
    index_types = get_index_types(columns, index_fields)
    unique_field = next((col["variable_name"] for col in columns if col["index_type"] == "unique"), None)
    unique_field_getter = f"get{to_camel_case(unique_field)[0].upper() + to_camel_case(unique_field)[1:]}" if unique_field else None

    # ソートとリミットの処理
    sort_str = ""
    limit_str = ""
    is_limit_one = False
    if "order" in query:
        sorts = query["order"].get("sort", [])
        sort_clauses = [f"{('ascending' if s['type'] == 'asc' else 'descending')}(\"{s['comparison']}\")" for s in sorts]
        if sort_clauses:
            sort_str = f".sort({', '.join(sort_clauses)})"
        if "order" in query and query["order"].get("limit") == 1:
            limit_str = ".limit(1)"
            is_limit_one = True

    # WHERE条件のフィルター生成
    param_variations = []
    filter_conditions = []
    for condition in where_conditions:
        field = condition["comparison"]
        param_name = f"where_{field.replace('.', '_')}"
        param_type = "String"
        is_array = False
        single_flag = condition.get("single_flag", False)
        match_type = condition.get("match_type", None)
        compar_type = condition.get("compar_type", "eq")

        for col in columns:
            if col["variable_name"] == field:
                param_type = generate_java_type(col["variable_type"])
                is_array = col["is_array"]
                if is_array and not single_flag:
                    param_type = f"List<{param_type}>"
                break
        if "." in field and "pos" in field:
            param_type = "Double"
        elif field == "zone_id":
            param_type = "Integer"
        elif field == "start_pos" or field == "end_pos":
            param_type = "Vector2"

        param_variations.append((param_type, param_name))

        if match_type == "ANY":
            filter_conditions.append(f'Filters.in("{field}", {param_name})')
        elif match_type == "ALL":
            filter_conditions.append(f'Filters.all("{field}", {param_name})')
        else:
            filter_map = {">=": "gte", "<=": "lte", ">": "gt", "<": "lt", "!=": "ne", "eq": "eq"}
            filter_conditions.append(
                f'Filters.{filter_map.get(compar_type, "eq")}("{field}", {param_name}' +
                ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
            )

    filter_str = ', '.join(filter_conditions)
    filter_clause = "new Document()" if not filter_conditions else (
        f"Filters.and({filter_str})" if len(filter_conditions) > 1 else filter_str
    )

    # キャッシュアクセスロジック（直接引数）
    cache_access = ""
    if not where_conditions:
        # where条件がない場合、cache_dataの全データを返す
        cache_access = f"""
            if (cache_data != null) {{
                List<{class_name}> resultList = new ArrayList<>(cache_data.values());
                return DataBaseResultPair.of(!resultList.isEmpty(), resultList);
            }}
"""
    elif any(cond["comparison"] in index_fields for cond in where_conditions):
        index_field = next((cond["comparison"] for cond in where_conditions if cond["comparison"] in index_fields), None)
        if index_field:
            param_name = f"where_{index_field.replace('.', '_')}"
            index_type = next((generate_java_type(col["variable_type"]) for col in columns if col["variable_name"] == index_field), "String")
            if len(index_fields) == 1:
                cache_access = f"""
                if (cache_data != null && cache_data.containsKey({param_name})) {{
                    {class_name} result = cache_data.get({param_name});
                    return DataBaseResultPair.of(result != null, result);
                }}
"""
            else:
                cache_access = f"""
                if (cache_data != null && cache_data.containsKey({param_name})) {{
                    Map<{index_types[1]}, {class_name}> innerMap = cache_data.get({param_name});
                    {class_name} result = innerMap != null ? innerMap.values().stream().findFirst().orElse(null) : null;
                    return DataBaseResultPair.of(result != null, result);
                }}
"""
    else:  # 非インデックスフィールドの場合
        field = where_conditions[0]["comparison"]
        param_name = f"where_{field.replace('.', '_')}"
        match_type = where_conditions[0].get("match_type", None)
        single_flag = where_conditions[0].get("single_flag", False)
        if is_limit_one:
            cache_access = f"""
                if (cache_data != null) {{
                    {class_name} result = cache_data.values().stream()
                        .filter(item -> item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}() != null && 
                            item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}().{'contains' if single_flag else 'containsAll'}({param_name}))
                        .findFirst().orElse(null);
                    return DataBaseResultPair.of(result != null, result);
                }}
"""
        else:
            cache_access = f"""
                if (cache_data != null) {{
                    List<{class_name}> resultList = cache_data.values().stream()
                        .filter(item -> item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}() != null && 
                            item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}().{'contains' if single_flag else 'containsAll'}({param_name}))
                        .collect(Collectors.toList());
                    return DataBaseResultPair.of(!resultList.isEmpty(), resultList);
                }}
"""

    # SELECTメソッド（直接引数）
    if query_type == "SELECT":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        methods.append(f"""
public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db{', ' + param_str if param_str else ''}) {{
    try {{
        {cache_access}
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        FindIterable<Document> results = collection.find({filter_clause}){sort_str}{limit_str};
        {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
    }}
}}""")

        # SELECTメソッド（ClientSession付き）
        methods.append(f"""
public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db, ClientSession session{', ' + param_str if param_str else ''}) {{
    try {{
        {cache_access}
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        FindIterable<Document> results = collection.find(session, {filter_clause}){sort_str}{limit_str};
        {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
    }}
}}""")

        # SELECTメソッド（set_data使用）
        extra_args = [(p[0], p[1]) for p in param_variations if any(c["comparison"] == p[1].replace("where_", "") and c.get("fixed_flag", False) for c in where_conditions)]
        extra_args_str = ', '.join(f"{arg[0]} {arg[1]}" for arg in extra_args)
        filter_str_data = []
        for cond in where_conditions:
            field = cond["comparison"]
            compar_type = cond.get("compar_type", "eq")
            match_type = cond.get("match_type", None)
            single_flag = cond.get("single_flag", False)
            filter_op = {"gte": "gte", "lte": "lte", "gt": "gt", "lt": "lt", "ne": "ne"}.get(compar_type, "eq")
            param_value = get_getter_name(field, "set_data") if not cond.get("fixed_flag", False) else f"where_{field.replace('.', '_')}"
            if match_type == "ANY":
                filter_str = f'Filters.in("{field}", {param_value})'
            elif match_type == "ALL":
                filter_str = f'Filters.all("{field}", {param_value})'
            else:
                filter_str = f'Filters.{filter_op}("{field}", {param_value}' + ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
            filter_str_data.append(filter_str)

        filter_str_data = ', '.join(filter_str_data)
        filter_clause_data = "new Document()" if not filter_str_data else (
            f"Filters.and({filter_str_data})" if len(where_conditions) > 1 else filter_str_data
        )

        # cache_access_data の定義
        cache_access_data = ""
        if not where_conditions:
            # where条件がない場合、cache_dataの全データを返す
            cache_access_data = f"""
                if (cache_data != null) {{
                    List<{class_name}> resultList = new ArrayList<>(cache_data.values());
                    return DataBaseResultPair.of(!resultList.isEmpty(), resultList);
                }}
"""
        elif any(cond["comparison"] in index_fields for cond in where_conditions):
            index_field = next((cond["comparison"] for cond in where_conditions if cond["comparison"] in index_fields), None)
            if index_field:
                index_type = next((generate_java_type(col["variable_type"]) for col in columns if col["variable_name"] == index_field), "String")
                if len(index_fields) == 1:
                    cache_access_data = f"""
                    if (cache_data != null && cache_data.containsKey(set_data.get{to_camel_case(index_field)[0].upper() + to_camel_case(index_field)[1:]}())) {{
                        {class_name} result = cache_data.get(set_data.get{to_camel_case(index_field)[0].upper() + to_camel_case(index_field)[1:]}());
                        return DataBaseResultPair.of(result != null, result);
                    }}
"""
                else:
                    cache_access_data = f"""
                    if (cache_data != null && cache_data.containsKey(String.valueOf(set_data.get{to_camel_case(index_fields[0])[0].upper() + to_camel_case(index_fields[0])[1:]}()))) {{
                        Map<{index_types[1]}, {class_name}> innerMap = cache_data.get(String.valueOf(set_data.get{to_camel_case(index_fields[0])[0].upper() + to_camel_case(index_fields[0])[1:]}()));
                        {class_name} result = innerMap != null ? innerMap.values().stream().findFirst().orElse(null) : null;
                        return DataBaseResultPair.of(result != null, result);
                    }}
"""
        else:  # 非インデックスフィールドの場合
            field = where_conditions[0]["comparison"]
            match_type = where_conditions[0].get("match_type", None)
            single_flag = where_conditions[0].get("single_flag", False)
            fixed_flag = where_conditions[0].get("fixed_flag", False)
            param_value = f"where_{field.replace('.', '_')}" if fixed_flag else f"set_data.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}()"
            if is_limit_one:
                cache_access_data = f"""
                    if (cache_data != null) {{
                        {class_name} result = cache_data.values().stream()
                            .filter(item -> item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}() != null && 
                                item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}().{'contains' if single_flag else 'containsAll'}({param_value}))
                            .findFirst().orElse(null);
                        return DataBaseResultPair.of(result != null, result);
                    }}
"""
            else:
                cache_access_data = f"""
                    if (cache_data != null) {{
                        List<{class_name}> resultList = cache_data.values().stream()
                            .filter(item -> item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}() != null && 
                                item.get{to_camel_case(field)[0].upper() + to_camel_case(field)[1:]}().{'contains' if single_flag else 'containsAll'}({param_value}))
                            .collect(Collectors.toList());
                        return DataBaseResultPair.of(!resultList.isEmpty(), resultList);
                    }}
"""

        methods.append(f"""
public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}) {{
    try {{
        {cache_access_data}
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        FindIterable<Document> results = collection.find({filter_clause_data}){sort_str}{limit_str};
        {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
    }}
}}""")

        methods.append(f"""
public static {return_type_single if is_limit_one else return_type_many} {method_name}{'One' if is_limit_one else 'Many'}(MongoDatabase db, ClientSession session, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}) {{
    try {{
        {cache_access_data}
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        FindIterable<Document> results = collection.find(session, {filter_clause_data}){sort_str}{limit_str};
        {'Document doc = results.first(); return DataBaseResultPair.of(doc != null, doc != null ? new ' + class_name + '(doc) : null);' if is_limit_one else f'List<{class_name}> resultList = new ArrayList<>(); for (Document doc : results) {{ resultList.add(new {class_name}(doc)); }} return DataBaseResultPair.of(!resultList.isEmpty(), resultList);'}
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, {'null' if is_limit_one else 'Collections.emptyList()'});
    }}
}}""")

    # UPDATEメソッド
    if query_type == "UPDATE":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        set_args = []
        set_clauses = query.get("set", [])
        update_strs = []
        for set_clause in set_clauses:
            field = set_clause["renewal"]
            details_type = set_clause.get("details_type")
            java_type = next((col["variable_type"] for col in columns if col["variable_name"] == field), "Object")
            java_type = generate_java_type(java_type)
            is_array = next((col["is_array"] for col in columns if col["variable_name"] == field), False)
            set_param_name = f"set_{field}"
            if details_type == "Add" and set_clause.get("fixed_flag", False) and not is_array:
                update_strs.append(f"inc(\"{field}\", {set_param_name})")
                set_args.append((java_type, set_param_name))
            elif details_type == "Subtract" and set_clause.get("fixed_flag", False) and not is_array:
                update_strs.append(f"inc(\"{field}\", -{set_param_name})")
                set_args.append((java_type, set_param_name))
            elif details_type == "Add":
                if set_clause.get("fixed_flag", False):
                    update_strs.append(f"addEachToSet(\"{field}\", {set_param_name})")
                    set_args.append((f"List<{java_type}>", set_param_name))
                else:
                    update_strs.append(f"addToSet(\"{field}\", {set_param_name})")
                    set_args.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
            elif details_type == "Delete":
                if set_clause.get("fixed_flag", False):
                    update_strs.append(f"pullAll(\"{field}\", {set_param_name})")
                    set_args.append((f"List<{java_type}>", set_param_name))
                else:
                    update_strs.append(f"pull(\"{field}\", {set_param_name})")
                    set_args.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
            else:
                update_strs.append(f"set(\"{field}\", {set_param_name})")
                set_args.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
        update_str = f"combine({', '.join(update_strs)})" if len(update_strs) > 1 else update_strs[0]
        extra_args_str = ', '.join(f"{arg[0]} {arg[1]}" for arg in set_args)

        cache_update = f"""
                if (memory_update && updatedDoc != null && cache_data != null && "{unique_field}" != null) {{
                    {class_name} updatedData = new {class_name}(updatedDoc);
                    {'cache_data.put(updatedData.' + unique_field_getter + '(), updatedData);' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.computeIfAbsent(String.valueOf(updatedData.' + get_getter_name(index_fields[0], 'updatedData') + '), k -> new HashMap<>()); innerMap.put(updatedData.' + get_getter_name(index_fields[1], 'updatedData') + ', updatedData);'}
                }}
"""
        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db{', ' + param_str if param_str else ''}{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document updatedDoc = collection.findOneAndUpdate({filter_clause}, Updates.{update_str}, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        {cache_update}
        return DataBaseResultPair.of(updatedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session{', ' + param_str if param_str else ''}{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document updatedDoc = collection.findOneAndUpdate(session, {filter_clause}, Updates.{update_str}, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        {cache_update}
        return DataBaseResultPair.of(updatedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        set_filter_str = []
        for cond in where_conditions:
            field = cond["comparison"]
            compar_type = cond.get("compar_type", "eq")
            match_type = cond.get("match_type", None)
            single_flag = cond.get("single_flag", False)
            filter_op = {"gte": "gte", "lte": "lte", "gt": "gt", "lt": "lt", "ne": "ne"}.get(compar_type, "eq")
            param_value = get_getter_name(field, "set_data") if not cond.get("fixed_flag", False) else f"where_{field.replace('.', '_')}"
            if match_type == "ANY":
                filter_str = f'Filters.in("{field}", {param_value})'
            elif match_type == "ALL":
                filter_str = f'Filters.all("{field}", {param_value})'
            else:
                filter_str = f'Filters.{filter_op}("{field}", {param_value}' + ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
            set_filter_str.append(filter_str)

        set_filter_str = ', '.join(set_filter_str)
        set_filter_clause = "new Document()" if not set_filter_str else (
            f"Filters.and({set_filter_str})" if len(where_conditions) > 1 else set_filter_str
        )
        set_update_strs = []
        set_args_data = []
        for set_clause in set_clauses:
            field = set_clause["renewal"]
            details_type = set_clause.get("details_type")
            java_type = next((col["variable_type"] for col in columns if col["variable_name"] == field), "Object")
            java_type = generate_java_type(java_type)
            is_array = next((col["is_array"] for col in columns if col["variable_name"] == field), False)
            set_param_name = f"set_{field}"
            getter_name = get_getter_name(field, "set_data")
            if details_type == "Add" and set_clause.get("fixed_flag", False) and not is_array:
                set_update_strs.append(f"inc(\"{field}\", {set_param_name})")
                set_args_data.append((java_type, set_param_name))
            elif details_type == "Subtract" and set_clause.get("fixed_flag", False) and not is_array:
                set_update_strs.append(f"inc(\"{field}\", -{set_param_name})")
                set_args_data.append((java_type, set_param_name))
            elif details_type == "Add":
                if set_clause.get("fixed_flag", False):
                    set_update_strs.append(f"addEachToSet(\"{field}\", {set_param_name})")
                    set_args_data.append((f"List<{java_type}>", set_param_name))
                else:
                    set_update_strs.append(f"addToSet(\"{field}\", {getter_name})")
            elif details_type == "Delete":
                if set_clause.get("fixed_flag", False):
                    set_update_strs.append(f"pullAll(\"{field}\", {set_param_name})")
                    set_args_data.append((f"List<{java_type}>", set_param_name))
                else:
                    set_update_strs.append(f"pull(\"{field}\", {getter_name})")
            else:
                set_update_strs.append(f"set(\"{field}\", {(set_param_name if set_clause.get('fixed_flag', False) else getter_name)})")
                if set_clause.get("fixed_flag", False):
                    set_args_data.append((is_array and f"List<{java_type}>" or java_type, set_param_name))
        set_update_str = f"combine({', '.join(set_update_strs)})" if len(set_update_strs) > 1 else set_update_strs[0]
        extra_args_str_data = ', '.join(f"{arg[0]} {arg[1]}" for arg in set_args_data)

        cache_update_data = f"""
                if (memory_update && updatedDoc != null && cache_data != null && "{unique_field}" != null) {{
                    {class_name} updatedData = new {class_name}(updatedDoc);
                    {'cache_data.put(updatedData.' + unique_field_getter + '(), updatedData);' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.computeIfAbsent(String.valueOf(updatedData.' + get_getter_name(index_fields[0], 'updatedData') + '), k -> new HashMap<>()); innerMap.put(updatedData.' + get_getter_name(index_fields[1], 'updatedData') + ', updatedData);'}
                }}
"""
        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, {class_name} set_data{', ' + extra_args_str_data if extra_args_str_data else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document updatedDoc = collection.findOneAndUpdate({set_filter_clause}, Updates.{set_update_str}, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        {cache_update_data}
        return DataBaseResultPair.of(updatedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {class_name} set_data{', ' + extra_args_str_data if extra_args_str_data else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document updatedDoc = collection.findOneAndUpdate(session, {set_filter_clause}, Updates.{set_update_str}, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        {cache_update_data}
        return DataBaseResultPair.of(updatedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

    # DELETEメソッド
    if query_type == "DELETE":
        param_str = ', '.join(f"{param[0]} {param[1]}" for param in param_variations)
        cache_delete = f"""
                if (memory_update && deletedDoc != null && cache_data != null && "{unique_field}" != null) {{
                    {class_name} deletedData = new {class_name}(deletedDoc);
                    {'cache_data.remove(deletedData.' + unique_field_getter + '());' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.get(String.valueOf(deletedData.' + get_getter_name(index_fields[0], 'deletedData') + ')); if (innerMap != null) innerMap.remove(deletedData.' + get_getter_name(index_fields[1], 'deletedData') + ');'}
                }}
"""
        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db{', ' + param_str if param_str else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document deletedDoc = collection.findOneAndDelete({filter_clause});
        {cache_delete}
        return DataBaseResultPair.of(deletedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session{', ' + param_str if param_str else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document deletedDoc = collection.findOneAndDelete(session, {filter_clause});
        {cache_delete}
        return DataBaseResultPair.of(deletedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        extra_args = [(p[0], p[1]) for p in param_variations if any(c["comparison"] == p[1].replace("where_", "") and c.get("fixed_flag", False) for c in where_conditions)]
        extra_args_str = ', '.join(f"{arg[0]} {arg[1]}" for arg in extra_args)
        set_filter_str = []
        for cond in where_conditions:
            field = cond["comparison"]
            compar_type = cond.get("compar_type", "eq")
            match_type = cond.get("match_type", None)
            single_flag = cond.get("single_flag", False)
            filter_op = {"gte": "gte", "lte": "lte", "gt": "gt", "lt": "lt", "ne": "ne"}.get(compar_type, "eq")
            param_value = get_getter_name(field, "set_data") if not cond.get("fixed_flag", False) else f"where_{field.replace('.', '_')}"
            if match_type == "ANY":
                filter_str = f'Filters.in("{field}", {param_value})'
            elif match_type == "ALL":
                filter_str = f'Filters.all("{field}", {param_value})'
            else:
                filter_str = f'Filters.{filter_op}("{field}", {param_value}' + ('.toDocument()' if field in ["start_pos", "end_pos"] else ')')
            set_filter_str.append(filter_str)

        set_filter_str = ', '.join(set_filter_str)
        set_filter_clause = "new Document()" if not set_filter_str else (
            f"Filters.and({set_filter_str})" if len(where_conditions) > 1 else set_filter_str
        )
        cache_delete_data = f"""
                if (memory_update && deletedDoc != null && cache_data != null && "{unique_field}" != null) {{
                    {class_name} deletedData = new {class_name}(deletedDoc);
                    {'cache_data.remove(deletedData.' + unique_field_getter + '());' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.get(String.valueOf(deletedData.' + get_getter_name(index_fields[0], 'deletedData') + ')); if (innerMap != null) innerMap.remove(deletedData.' + get_getter_name(index_fields[1], 'deletedData') + ');'}
                }}
"""
        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document deletedDoc = collection.findOneAndDelete({set_filter_clause});
        {cache_delete_data}
        return DataBaseResultPair.of(deletedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {class_name} set_data{', ' + extra_args_str if extra_args_str else ''}, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        Document deletedDoc = collection.findOneAndDelete(session, {set_filter_clause});
        {cache_delete_data}
        return DataBaseResultPair.of(deletedDoc != null, null);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

    # INSERTメソッド
    if query_type == "INSERT":
        cache_insert = f"""
                if (memory_update && cache_data != null) {{
                    {'cache_data.put(data.' + unique_field_getter + '(), data);' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.computeIfAbsent(String.valueOf(data.' + get_getter_name(index_fields[0], 'data') + '), k -> new HashMap<>()); innerMap.put(data.' + get_getter_name(index_fields[1], 'data') + ', data);'}
                }}
"""
        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, {class_name} data, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        collection.insertOne(data.toDocument());
        {cache_insert}
        return DataBaseResultPair.of(true, data);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

        methods.append(f"""
public static {return_type_single} {method_name}(MongoDatabase db, ClientSession session, {class_name} data, boolean memory_update) {{
    try {{
        MongoCollection<Document> collection = db.getCollection("{collection_name}");
        collection.insertOne(session, data.toDocument());
        {cache_insert}
        return DataBaseResultPair.of(true, data);
    }} catch (Exception e) {{
        return DataBaseResultPair.of(false, null);
    }}
}}""")

    return "\n".join(methods)



def generate_db_class(class_name, queries, columns, collection_name):
    query_methods = "\n".join(generate_query_methods(query, collection_name, class_name, columns) for query in queries) if queries else ""
    
    # インデックスフィールドを取得（優先順位: unique > hash > index）
    index_fields = []
    for col in columns:
        if col["index_type"] == "unique":
            index_fields.insert(0, col["variable_name"])
        elif col["index_type"] == "hash":
            index_fields.append(col["variable_name"])
        elif col["index_type"] == "index":
            index_fields.append(col["variable_name"])
    index_types = get_index_types(columns, index_fields)

    # キャッシュデータ型を動的に生成
    cache_type = f"Map<{index_types[0]}, {class_name}>" if len(index_fields) == 1 else f"Map<{index_types[0]}, Map<{index_types[1]}, {class_name}>>"
    
    # キャッシュ初期化ロジック
    cache_init = f"""
    cache_data = new HashMap<>();
    MongoCollection<Document> collection = db.getCollection(collection_name);
    FindIterable<Document> results = collection.find();
    for (Document doc : results) {{
        {class_name} data = new {class_name}(doc);
        {'cache_data.put(' + get_getter_name(index_fields[0], 'data') + ', data);' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.computeIfAbsent(String.valueOf(data.' + get_getter_name(index_fields[0], 'data') + '), k -> new HashMap<>()); innerMap.put(String.valueOf(data.' + get_getter_name(index_fields[1], 'data') + '), data);'}
    }}
"""

    # インデックス作成
    index_creation = ''.join(f'collection.createIndex(Indexes.ascending("{col["variable_name"]}"), new IndexOptions().unique({"true" if col["index_type"] == "unique" else "false"}));' for col in columns if col["index_type"] != "none")

    return f"""import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class {class_name.replace("Data","")}Db {{
    public static final String collection_name = "{collection_name}";
    public static {cache_type} cache_data;

    public static void MemoryCache{class_name}(MongoDatabase db) {{{cache_init}
    }}

    public static void createIndexes(MongoDatabase db) {{
        MongoCollection<Document> collection = db.getCollection(collection_name);
        {index_creation}
    }}

    public static boolean bulkInsert{class_name}(MongoDatabase db, List<{class_name}> dataList) {{
        try {{
            if(cache_data == null) cache_data = new HashMap<>();
            MongoCollection<Document> collection = db.getCollection(collection_name);
            List<Document> documents = new ArrayList<>();
            for ({class_name} data : dataList) {{
                documents.add(data.toDocument());
                {'cache_data.put(' + get_getter_name(index_fields[0], 'data') + ', data);' if len(index_fields) == 1 else f'Map<{index_types[1]}, {class_name}> innerMap = cache_data.computeIfAbsent(String.valueOf(data.' + get_getter_name(index_fields[0], 'data') + '), k -> new HashMap<>()); innerMap.put(String.valueOf(data.' + get_getter_name(index_fields[1], 'data') + '), data);'}
            }}
            collection.insertMany(documents);
            return true;
        }} catch (com.mongodb.MongoWriteException e) {{
            if (e.getCode() == 11000) {{
                return false;
            }}
            throw e;
        }} catch (Exception e) {{
            return false;
        }}
    }}

    {query_methods}
}}
"""
def generate_java_class(class_name, columns):
    fields = "\n".join(generate_field_declaration(col) for col in columns)
    getters = "\n".join(generate_getter(col) for col in columns)
    setters = "\n".join(generate_setter(col) for col in columns)
    doc_constructor_lines = []
    for col in columns:
        var_name = col["variable_name"]
        var_type = col["variable_type"]
        if col["is_array"]:
            doc_constructor_lines.append(f"        this.{var_name} = (List<{generate_java_type(var_type)}>) doc.get(\"{var_name}\");")
        elif var_type == "int":
            doc_constructor_lines.append(f"        this.{var_name} = doc.getInteger(\"{var_name}\");")
        elif var_type == "String":
            doc_constructor_lines.append(f"        this.{var_name} = doc.getString(\"{var_name}\");")
        elif var_type == "double":
            doc_constructor_lines.append(f"        this.{var_name} = doc.getDouble(\"{var_name}\");")
        else:
            doc_constructor_lines.append(f"        Document {var_name}Doc = (Document) doc.get(\"{var_name}\");")
            doc_constructor_lines.append(f"        this.{var_name} = {var_name}Doc != null ? new {generate_java_type(var_type)}({var_name}Doc) : null;")
    doc_constructor = "\n".join(doc_constructor_lines)

    to_doc_lines = ["        Document doc = new Document();"]
    for col in columns:
        var_name = col["variable_name"]
        var_type = col["variable_type"]
        if col["is_array"] or var_type in ["int", "String", "double"]:
            to_doc_lines.append(f"        doc.append(\"{var_name}\", this.{var_name});")
        else:
            to_doc_lines.append(f"        doc.append(\"{var_name}\", this.{var_name} != null ? this.{var_name}.toDocument() : null);")
    to_doc_lines.append("        return doc;")
    to_doc = "\n".join(to_doc_lines)

    return f"""import org.bson.Document;
import java.util.List;

public class {class_name} {{
{fields}

    public {class_name}() {{
    }}

    public {class_name}(Document doc) {{{doc_constructor}
    }}

    public Document toDocument() {{{to_doc}
    }}

    {getters}
    {setters}
}}
"""

def generate_java_code(json_data, collection_name):
    job_type = json_data[collection_name]
    main_class_name = generate_class_name(collection_name)
    main_columns = job_type["column_list"]
    queries = job_type.get("queries", [])

    with open(f"{main_class_name}.java", "w") as f:
        f.write(generate_java_class(main_class_name, main_columns))
    print(f"Generated {main_class_name}.java")

    with open(f"{main_class_name.replace('Data','')}Db.java", "w") as f:
        f.write(generate_db_class(main_class_name, queries, main_columns, collection_name))
    print(f"Generated {main_class_name.replace('Data','')}Db.java")

    if "customVariables" in job_type:
        for custom_var in job_type["customVariables"]:
            for var_type, columns in custom_var.items():
                custom_class_name = generate_class_name(var_type)
                with open(f"{custom_class_name}.java", "w") as f:
                    f.write(generate_java_class(custom_class_name, columns))
                print(f"Generated {custom_class_name}.java")

def main():

    
    json_data = {
        "item_block_game": {
            "column_list": [
                {"variable_type": "String", "variable_name": "id_name", "variable_explanation": "ID名", "index_type": "unique", "is_array": False},
                {"variable_type": "String", "variable_name": "name_lang", "variable_explanation": "日本語名", "index_type": "none", "is_array": False},
                {"variable_type": "double", "variable_name": "rate", "variable_explanation": "レート", "index_type": "none", "is_array": False},
                {"variable_type": "double", "variable_name": "price", "variable_explanation": "価格", "index_type": "none", "is_array": False},
                {"variable_type": "int", "variable_name": "recipe_create_job_list", "variable_explanation": "作れる職業jobリスト", "index_type": "none", "is_array": True},
                {"variable_type": "int", "variable_name": "action_price_job", "variable_explanation": "アクションしたとき得られるジョブ", "index_type": "none", "is_array": True},
                {"variable_type": "String", "variable_name": "tags_list", "variable_explanation": "タグリスト", "index_type": "none", "is_array": True},
            ],
            "queries": [
                {"type": "SELECT", "method_name": "findAllItemBlock"},
                 {"type": "SELECT", "where": [{"comparison": "id_name"}],  "order": {"limit": 1},"method_name": "findIdName"},
                {"type": "UPDATE", "set": [{"renewal": "recipe_create_job_list", "details_type": "Add"}], "where": [{"comparison": "id_name"}], "method_name": "AddRecipeJob"},
                {"type": "SELECT", "where": [{"comparison": "recipe_create_job_list", "match_type": "ANY", "fixed_flag": True, "single_flag": True}], "method_name": "findJobRecipe"},
                {"type": "SELECT", "where": [{"comparison": "action_price_job", "match_type": "ANY", "fixed_flag": True, "single_flag": True}],                "order": {
                    "limit": 1
                }, "method_name": "findActionJob"},
                {"type": "SELECT", "where": [{"comparison": "action_price_job", "match_type": "ANY", "fixed_flag": True, "single_flag": True},
                                             {"conditions": "and","comparison": "id_name", "fixed_flag": True, "single_flag": True}],
                 "order": {
                    "limit": 1
                }, "method_name": "findActionBlockJob"},
                {"type": "SELECT", "where": [{"comparison": "tags_list", "match_type": "ANY", "fixed_flag": True}], "method_name": "findTags"},
            ]
        }
    }
    


    
    generate_java_code(json_data, "item_block_game")

if __name__ == "__main__":
    main()