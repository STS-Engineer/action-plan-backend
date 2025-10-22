// server.js - Backend API pour Action Plan DB (corrigé)
require('dotenv').config({ path: __dirname + '/.env' }); // 1) Charger .env en tout premier

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const PORT = Number(process.env.PORT || 5000);

// 2) Aucune valeur en dur / fallback : on lit UNIQUEMENT process.env
// 3) SSL obligatoire sur Azure PostgreSQL
const pool = new Pool({
  user: process.env.DB_USER,            // ex: administrationSTS@avo-adb-002
  host: process.env.DB_HOST,            // ex: avo-adb-002.postgres.database.azure.com
  database: process.env.DB_NAME,        // ex: Action Plan
  password: process.env.DB_PASSWORD,    // ex: St$@0987
  port: Number(process.env.DB_PORT || 5432),
  ssl: { require: true, rejectUnauthorized: false }
});

// Petit log de contrôle au démarrage (à retirer ensuite)
console.log('🔧 DB config (sanity check):', {
  DB_USER: process.env.DB_USER,
  DB_HOST: process.env.DB_HOST,
  DB_NAME: process.env.DB_NAME,
  DB_PORT: process.env.DB_PORT
});

// Middleware
app.use(cors());
app.use(express.json());

// Test de connexion à la base de données
pool.connect()
  .then(client => {
    console.log('✅ Connexion à PostgreSQL réussie');
    client.release();
  })
  .catch(err => {
    console.error('❌ Erreur de connexion à PostgreSQL:', err);
  });

// ==================== ROUTES API ====================

// Route de test
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', message: 'API Action Plan est en ligne' });
});

// 1. Récupérer tous les sujets (avec statistiques)
app.get('/api/sujets', async (req, res) => {
  try {
    const query = `
      SELECT 
        s.*,
        COUNT(DISTINCT a.id) as total_actions,
        COUNT(DISTINCT CASE WHEN a.status = 'completed' THEN a.id END) as completed_actions,
        COUNT(DISTINCT CASE WHEN a.status = 'overdue' THEN a.id END) as overdue_actions
      FROM sujet s
      LEFT JOIN action a ON s.id = a.sujet_id
      GROUP BY s.id
      ORDER BY s.created_at DESC
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Erreur lors de la récupération des sujets:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 2. Récupérer un sujet spécifique avec ses détails
app.get('/api/sujets/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const sujetQuery = await pool.query('SELECT * FROM sujet WHERE id = $1', [id]);
    
    if (sujetQuery.rows.length === 0) {
      return res.status(404).json({ error: 'Sujet non trouvé' });
    }

    res.json(sujetQuery.rows[0]);
  } catch (err) {
    console.error('Erreur lors de la récupération du sujet:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 3. Récupérer les sous-sujets d'un sujet parent
app.get('/api/sujets/:id/sous-sujets', async (req, res) => {
  try {
    const { id } = req.params;
    const query = `
      SELECT 
        s.*,
        COUNT(DISTINCT a.id) as total_actions,
        COUNT(DISTINCT CASE WHEN a.status = 'completed' THEN a.id END) as completed_actions
      FROM sujet s
      LEFT JOIN action a ON s.id = a.sujet_id
      WHERE s.parent_sujet_id = $1
      GROUP BY s.id
      ORDER BY s.created_at DESC
    `;
    const result = await pool.query(query, [id]);
    res.json(result.rows);
  } catch (err) {
    console.error('Erreur lors de la récupération des sous-sujets:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 4. Récupérer les sujets racines (sans parent)
app.get('/api/sujets-racines', async (req, res) => {
  try {
    const query = `
      SELECT 
        s.*,
        COUNT(DISTINCT a.id) as total_actions,
        COUNT(DISTINCT ss.id) as total_sous_sujets
      FROM sujet s
      LEFT JOIN action a ON s.id = a.sujet_id
      LEFT JOIN sujet ss ON s.id = ss.parent_sujet_id
      WHERE s.parent_sujet_id IS NULL
      GROUP BY s.id
      ORDER BY s.created_at DESC
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Erreur lors de la récupération des sujets racines:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 5. Récupérer toutes les actions d'un sujet
app.get('/api/sujets/:id/actions', async (req, res) => {
  try {
    const { id } = req.params;
    const query = `
      SELECT * FROM action 
      WHERE sujet_id = $1 AND parent_action_id IS NULL
      ORDER BY ordre ASC, created_at DESC
    `;
    const result = await pool.query(query, [id]);
    res.json(result.rows);
  } catch (err) {
    console.error('Erreur lors de la récupération des actions:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 6. Récupérer une action spécifique
app.get('/api/actions/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM action WHERE id = $1', [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Action non trouvée' });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error('Erreur lors de la récupération de l\'action:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 7. Récupérer les sous-actions d'une action parent
app.get('/api/actions/:id/sous-actions', async (req, res) => {
  try {
    const { id } = req.params;
    const query = `
      SELECT * FROM action 
      WHERE parent_action_id = $1
      ORDER BY ordre ASC, created_at DESC
    `;
    const result = await pool.query(query, [id]);
    res.json(result.rows);
  } catch (err) {
    console.error('Erreur lors de la récupération des sous-actions:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// 8. Récupérer les statistiques globales
app.get('/api/statistiques', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT 
        COUNT(DISTINCT s.id) as total_sujets,
        COUNT(DISTINCT a.id) as total_actions,
        COUNT(DISTINCT CASE WHEN a.status = 'completed' THEN a.id END) as actions_completed,
        COUNT(DISTINCT CASE WHEN a.status = 'overdue' THEN a.id END) as actions_overdue,
        COUNT(DISTINCT CASE WHEN a.status = 'in_progress' THEN a.id END) as actions_in_progress,
        COUNT(DISTINCT CASE WHEN a.status = 'nouveau' THEN a.id END) as actions_nouveau
      FROM sujet s
      LEFT JOIN action a ON s.id = a.sujet_id
    `);
    res.json(stats.rows[0]);
  } catch (err) {
    console.error('Erreur lors de la récupération des statistiques:', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// Démarrage du serveur
app.listen(PORT, () => {
  console.log(`🚀 Serveur API démarré sur le port ${PORT}`);
  console.log(`📊 Documentation: http://localhost:${PORT}/api/health`);
});

// Gestion de la fermeture propre
process.on('SIGINT', async () => {
  console.log('\n🛑 Arrêt du serveur...');
  await pool.end();
  process.exit(0);
});
