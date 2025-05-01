# Plano de implantação da base sqlite3 no scheduler:

- [ ] Criar e testar funções CRUD
  - [x] fetch_jobs()
  - [ ] log_to_database()
- [x] Adaptar função que agenda as tarefas
  - [x] schedule_jobs()
  - [x] cria lógica para executar em threads
- [ ] Classe Logger
  - [x] Remover Classe
  - [ ] Retrabalhar em formato mais simples com CRUD na base.
  - [ ] Retrabalhar o identificador log_level
  - [ ] Criar logs de alterações de propriedades, exclusões e etc... 
- [ ] Criar tela de execução de eventuais. (moderadores)
- [ ] Criar tela de log no front:
  - [ ] Log individual do job. (normal)
  - [ ] Logs de execução, erros. (moderador)
  - [ ] Logs de criação de jobs, alteração de propriedades, logs de sistema. (root)
  - [ ] ~~Ao criar tarefa, ou ao ativar, agendar no scheduler.~~
  - [ ] Criar tarefa que agenda os jobs em um periodo menor, duas ou cinco horas.

  >   Importante: Só agendar na criação quando já estiver com o flag job_status: Y
  
    - para agendar no flask:
    ```python
    @app.route('/api/jobs', methods=['POST'])
    def create_job():
    data = request.json
    # ... lógica de inserir no DB ...
    db.session.commit()
    new_id = new_job.job_id
  
    # em vez de esperar o refresh diário, já agenda agora:
    from seu_scheduler import schedule_job
    schedule_job(new_id)
  
    return jsonify({'job_id': new_id}), 201
    ```
    em delete:
    ```python
    # após db.session.commit():
    schedule_job(job_id)
    # possivelmente tem que remover da agenda o job antigo
    ```
    em update:
    ```python
    # após remover do DB e commit:
    schedule.clear(f"job-{job_id}")
    ```

- [ ] Fim :D