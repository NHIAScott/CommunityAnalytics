export default function MetricsDictionaryPage() {
  return (
    <div>
      <h2>Metrics Dictionary</h2>
      <ul>
        <li>Engagement Score (0-100): weighted actions (logins, downloads, replies, threads, blogs, best answers, friendships), winsorized at p99 and min-max scaled per period.</li>
        <li>Super-User Score (0-100): engagement raw + network centrality blend, winsorized and scaled.</li>
        <li>Company Health Score (0-100): breadth/depth/contribution/network/consistency composite with explainable risk flags.</li>
        <li>Topic Influence: lift vs baseline and lead/lag stats where available.</li>
      </ul>
    </div>
  );
}
