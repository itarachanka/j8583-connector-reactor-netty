import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Production-grade TCP client',
    Svg: require('@site/static/img/undraw_mountain.svg').default,
    description: (
      <>
        Boilerplate project was designed from the ground up to be easily extended and
        used to get your microservice up and running quickly.
      </>
    ),
  },
  {
    title: 'Non-blocking I/O architecture',
    Svg: require('@site/static/img/undraw_tree.svg').default,
    description: (
      <>
        Using Reactor Netty for high-throughput ingestion.
        Backpressure mechanisms to prevent memory exhaustion under load spikes.
      </>
    ),
  },
  {
    title: 'Complete observability stack',
    Svg: require('@site/static/img/undraw_metrics.svg').default,
    description: (
      <>
        TCP client metrics, connection health checks, internal cache statistics.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
