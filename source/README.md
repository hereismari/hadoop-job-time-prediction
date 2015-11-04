#Classes used in the Experiment

The original code is property of Apache and it was modified in order to run trough Sahara as a JavaActionJob using Oozie.

- [Original code from Apache](https://svn.apache.org/repos/asf/hadoop/common/branches/branch-1/src/examples/org/apache/hadoop/examples/)
- Only a line was added in the java files. The modification below allows any configuration values from the <configuration> tag in an Oozie workflow to be set in the Configuration object.

  ```
  // This will add properties from the <configuration> tag specified
  // in the Oozie workflow.  For java actions, Oozie writes the
  // configuration values to a file pointed to by ooze.action.conf.xml
  conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
  ```

**PS : In the original experiment other confidentials job, that aren't available here were also executed**

## Generating the jar

To generate the jar file you must:

1. Compile the classes
2. Generate the jar

You can get help in this tasks with this [link](https://github.com/openstack/sahara/tree/master/etc/edp-examples/edp-java) :).
