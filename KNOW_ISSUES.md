# Erros e bugs conhecidos no projeto

1. **Erro no update de um job**

    Ao atualizar um job e não realizar alterações apenas nos campos de dia, hora e minuto, o job não atualiza.

    Isso acontece devido ao método de verificação, a função: `update_job()` verifica apenas alguns campos, se o campo modificado não está contemplado no check, então ele não atualiza.

    ```python
    # Track changes for logging
        changes = []
        for field in ['job_name', 'job_status', 'export_type', 'export_path', 'export_name', 'days_offset', 'check_parameter', 'parameter_id', 'data_primary_key', 'sql_script']:
            if field in data and getattr(j, field) != data[field]:
                 changes.append(f"{field} changed") # Could log old/new values if needed
                 setattr(j, field, data[field])
    ```

