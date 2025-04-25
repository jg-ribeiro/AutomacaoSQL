# Plano de implantação da base sqlite3 no scheduler:

- [ ] Criar e testar funções CRUD
  - [ ] fetch_jobs()
  - [ ] log_to_database()
- [ ] Adaptar função que agenda as tarefas
  - [ ] schedule_jobs()
- [ ] Classe Logger
  - [ ] Remover Classe
  - [ ] Retrabalhar em formato mais simples com CRUD na base.
  - [ ] Retrabalhar o identificador log_level
  - [ ] Criar logs de alterações de propriedades, exclusões e etc... 
- [ ] Criar tela de execução de eventuais. (moderadores)
- [ ] Criar tela de log no front:
  - [ ] Log individual do job. (normal)
  - [ ] Logs de execução, erros. (moderador)
  - [ ] Logs de criação de jobs, alteração de propriedades, logs de sistema. (root)
- [ ] Ao criar tarefa, ou ao ativar, agendar no scheduler.

  > Importante: Só agendar na criação quando já estiver com o flag job_status: Y
- [ ] 