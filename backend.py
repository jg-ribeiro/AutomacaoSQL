from flask import Flask, jsonify, request, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import os

# Configurações iniciais do Flask
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'frontend', 'dist')
app = Flask(__name__, static_folder=template_dir, static_url_path='')
CORS(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///C:/Auteq/AutomacaoSQL/jobs.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Models correspondendo às tabelas SQLite
class Parameter(db.Model):
    __tablename__ = 'parameters'
    parameter_id    = db.Column(db.Integer, primary_key=True)
    parameter_name  = db.Column(db.Text, nullable=False)
    sql_script      = db.Column(db.Text)
    jobs            = db.relationship('JobHE', backref='parameter', lazy=True)

class JobHE(db.Model):
    __tablename__ = 'jobs_he'
    job_id           = db.Column(db.Integer, primary_key=True)
    job_name         = db.Column(db.Text, nullable=False)
    export_type      = db.Column(db.Text, nullable=False)
    export_path      = db.Column(db.Text, nullable=False)
    export_name      = db.Column(db.Text, nullable=False)
    check_parameter = db.Column(db.Text)
    parameter_id     = db.Column(db.Integer, db.ForeignKey('parameters.parameter_id'))
    data_primary_key = db.Column(db.Text)
    sql_script       = db.Column(db.Text)
    schedule         = db.relationship('JobDE', uselist=False, backref='job')

class JobDE(db.Model):
    __tablename__ = 'jobs_de'
    schedule_id = db.Column(db.Integer, primary_key=True)
    job_id      = db.Column(db.Integer, db.ForeignKey('jobs_he.job_id'))
    job_minute  = db.Column(db.Text,  nullable=False)
    job_hour    = db.Column(db.Text,  nullable=False)
    job_day     = db.Column(db.Text,  nullable=False)

# Endpoints da API
@app.route('/api/parameters', methods=['GET'])
def list_parameters():
    params = Parameter.query.all()
    return jsonify([
        {'parameter_id': p.parameter_id, 'parameter_name': p.parameter_name}
        for p in params
    ])

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    jobs = JobHE.query.all()
    result = []
    for j in jobs:
        # puxa todos os schedules
        scheds = JobDE.query.filter_by(job_id=j.job_id).all()
        minutes = [s.job_minute for s in scheds]
        hours   = [s.job_hour   for s in scheds]
        days    = set([s.job_day     for s in scheds])
        result.append({
            'job_id': j.job_id,
            'job_name': j.job_name,
            'export_type': j.export_type,
            'export_path': j.export_path,
            'export_name': j.export_name,
            'check_parameter': j.check_parameter,
            'parameter_id': j.parameter_id,
            'data_primary_key': j.data_primary_key,
            'sql_script': j.sql_script,
            'schedule': {
                # dias podem repetir se o mesmo dia tiver múltiplos minutos/horas?
                'day': ','.join(days),
                # minutos e horas trazemos únicos (mantendo ordem de aparição)
                'minute': ','.join(dict.fromkeys(minutes)),
                'hour':   ','.join(dict.fromkeys(hours))
            }
        })
    return jsonify(result)

@app.route('/api/jobs', methods=['POST'])
def create_job():
    data = request.json
    # Cria JobHE
    new_job = JobHE(
        job_name=data['job_name'],
        export_type=data['export_type'],
        export_path=data['export_path'],
        export_name=data['export_name'],
        check_parameter=data.get('check_parameter'),
        parameter_id=data.get('parameter_id'),
        data_primary_key=data.get('data_primary_key'),
        sql_script=data.get('sql_script')
    )
    db.session.add(new_job)
    db.session.flush()  # obter job_id antes de commit para FK
    # Cria JobDE (produto cartesiano) se há schedule
    sched = data.get('schedule', {})
    minutes = sched.get('minute', '').split(',').filter(bool)
    hours   = sched.get('hour',   '').split(',').filter(bool)
    days    = sched.get('day',    '').split(',').filter(bool)
    for d in days:
        for h in hours:
            for m in minutes:
                db.session.add(JobDE(
                    job_id=new_job.job_id,
                    job_minute=m,
                    job_hour=h,
                    job_day=d
                ))

    db.session.commit()
    return jsonify({'job_id': new_job.job_id}), 201

@app.route('/api/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):
    j = JobHE.query.get_or_404(job_id)
    scheds = JobDE.query.filter_by(job_id=job_id).all()
    return jsonify({
        'job_id': j.job_id,
        'job_name': j.job_name,
        'export_type': j.export_type,
        'export_path': j.export_path,
        'export_name': j.export_name,
        'check_parameter': j.check_parameter,
        'parameter_id': j.parameter_id,
        'data_primary_key': j.data_primary_key,
        'sql_script': j.sql_script,
        'schedule': {
            'minute': ','.join([s.job_minute for s in scheds]),
            'hour': ','.join([s.job_hour   for s in scheds]),
            'day': ','.join(set([s.job_day     for s in scheds])),
        }
    })

@app.route('/api/jobs/<int:job_id>', methods=['PUT'])
def update_job(job_id):
    data = request.json
    j = JobHE.query.get_or_404(job_id)
    # atualiza campos
    j.job_name = data['job_name']
    j.export_type = data['export_type']
    j.export_path = data['export_path']
    j.export_name = data['export_name']
    j.check_parameter = data['check_parameter']
    j.parameter_id = data.get('parameter_id')
    j.data_primary_key = data.get('data_primary_key')
    j.sql_script = data.get('sql_script')
    # remove agendas antigas
    JobDE.query.filter_by(job_id=job_id).delete()
    # recria todas as combinações de day × hour × minute
    sched = data.get('schedule', {})
    minutes =  [x for x in sched.get('minute', '').split(',') if x]
    hours   =  [x for x in sched.get('hour',   '').split(',') if x]
    days    =  [x for x in set(sched.get('day',    '').split(',')) if x] #Adicionado set() para remover suplicados
    for d in days:
        for h in hours:
            for m in minutes:
                print(d, h, m)
                db.session.add(JobDE(
                    job_id=job_id,
                    job_minute=m,
                    job_hour=h,
                    job_day=d
                ))
    db.session.commit()
    return jsonify({'msg':'updated'})

# Servindo o front-end React+Vite
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path):
    # Se existir arquivo estático, serve-o, senão serve o index.html
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    # Cria as tabelas no banco, se não existirem
    #db.create_all()
    app.run(debug=True, port=5000)
