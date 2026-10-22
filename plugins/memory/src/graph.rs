use crate::{Memory, content::*, store::*, types::*};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Deserialize)]
pub struct Retrieval {
    pub seed_threshold: f64,
    pub seed_limit: usize,
    pub sequence_weight: f64,
    pub association_weight: f64,
    pub activation_min: f64,
    pub association_limit: usize,
    pub node_limit: usize,
    pub edge_limit: usize,
    pub result_limit: usize,
}
impl Retrieval {
    pub fn parse(text: &str) -> Result<Self, String> {
        let result: Self = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if !(0.0..=1.0).contains(&result.seed_threshold)
            || !(0.0..1.0).contains(&result.sequence_weight)
            || !(0.0..1.0).contains(&result.association_weight)
            || !(0.0..=1.0).contains(&result.activation_min)
            || result.seed_limit == 0
            || result.result_limit < result.seed_limit
            || result.node_limit < result.result_limit
        {
            return Err("invalid recall bounds/weights".into());
        }
        Ok(result)
    }
}
#[derive(Clone, Serialize)]
struct Edge {
    from: MemoryId,
    to: MemoryId,
    kind: &'static str,
    weight: f64,
}
impl Memory {
    pub async fn region(&self, seeds: Vec<(MemoryId, f64)>) -> Result<Region, Error> {
        let mut strengths: BTreeMap<MemoryId, f64> =
            seeds.iter().map(|(id, _)| (id.clone(), 1.0)).collect();
        let mut visited = BTreeSet::new();
        let mut edges = Vec::new();
        while visited.len() < self.retrieval.node_limit && edges.len() < self.retrieval.edge_limit {
            let current = strengths
                .iter()
                .filter(|(id, _)| !visited.contains(*id))
                .max_by(|a, b| a.1.total_cmp(b.1).then(b.0.cmp(a.0)))
                .map(|(id, value)| (id.clone(), *value));
            let Some((id, activation)) = current else {
                break;
            };
            visited.insert(id.clone());
            let remaining = self.retrieval.edge_limit - edges.len();
            let neighbors = self
                .neighbors(&id, self.retrieval.association_limit.min(remaining))
                .await?;
            for edge in neighbors.into_iter().take(remaining) {
                let value = activation * edge.weight;
                if value >= self.retrieval.activation_min
                    && (strengths.contains_key(&edge.to)
                        || strengths.len() < self.retrieval.node_limit)
                {
                    strengths
                        .entry(edge.to.clone())
                        .and_modify(|v| *v = v.max(value))
                        .or_insert(value);
                }
                edges.push(edge);
            }
        }
        let mut selected = strengths.iter().collect::<Vec<_>>();
        selected.sort_by(|a, b| b.1.total_cmp(a.1).then(a.0.cmp(b.0)));
        selected.truncate(self.retrieval.result_limit);
        let ids = selected
            .iter()
            .map(|(id, _)| (*id).clone())
            .collect::<Vec<_>>();
        let units = read(&ids, true).await?;
        let kept = units.iter().map(|u| u.id.clone()).collect::<BTreeSet<_>>();
        let edges = edges
            .into_iter()
            .filter(|e| kept.contains(&e.from) && kept.contains(&e.to))
            .collect::<Vec<_>>();
        let graph = serde_json::json!({"nodes":selected.iter().map(|(id,activation)|serde_json::json!({"id":id,"activation":activation,"seed_score":seeds.iter().find(|(s,_)|s==*id).map(|(_,s)|s)})).collect::<Vec<_>>(),"edges":edges});
        Ok(Region {
            units,
            seeds: seeds
                .into_iter()
                .map(|(id, _)| id)
                .filter(|id| kept.contains(id))
                .collect(),
            graph_json: graph.to_string(),
        })
    }
    async fn neighbors(&self, id: &MemoryId, limit: usize) -> Result<Vec<Edge>, Error> {
        let rows=execute(vec![
            stmt("SELECT sequence FROM units WHERE group_id=? AND archived=1 AND sequence<? ORDER BY sequence DESC LIMIT 1",vec![int(id.group),int(id.sequence)]),
            stmt("SELECT sequence FROM units WHERE group_id=? AND archived=1 AND sequence>? ORDER BY sequence LIMIT 1",vec![int(id.group),int(id.sequence)]),
            stmt("SELECT n.g,n.s FROM (SELECT recalled_group AS g,recalled_seq AS s FROM links WHERE trigger_group=? AND trigger_seq=? UNION SELECT trigger_group AS g,trigger_seq AS s FROM links WHERE recalled_group=? AND recalled_seq=?) AS n JOIN units AS u ON u.group_id=n.g AND u.sequence=n.s WHERE u.archived=1 ORDER BY random() LIMIT ?",vec![int(id.group),int(id.sequence),int(id.group),int(id.sequence),int(limit as i64)]),
        ]).await?;
        let mut edges = Vec::new();
        for row in rows[..2].iter().flat_map(|r| &r.rows) {
            let sequence = number(&row[0])?;
            edges.push(Edge {
                from: id.clone(),
                to: MemoryId {
                    group: id.group,
                    sequence,
                },
                kind: "sequence",
                weight: self
                    .retrieval
                    .sequence_weight
                    .powf(sequence.abs_diff(id.sequence) as f64),
            });
        }
        for row in &rows[2].rows {
            edges.push(Edge {
                from: id.clone(),
                to: MemoryId {
                    group: number(&row[0])?,
                    sequence: number(&row[1])?,
                },
                kind: "association",
                weight: self.retrieval.association_weight,
            });
        }
        Ok(edges)
    }
}
