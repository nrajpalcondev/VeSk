package conductor.verifier;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * A simple implementation of {@link VerificationContext} that stores the corrupt and missing rows in memory.
 */
public class BasicVerificationContext implements VerificationContext {

    private List<String> missingRows = Lists.newArrayList();
    private List<String> corruptRows = Lists.newArrayList();

    /**
     * Returns true if and only if the missing and corrupted row collections are empty.
     *
     * @return the verification status
     */
    public boolean verified() {
        return missingRows.isEmpty() && corruptRows.isEmpty();
    }

    /**
     * Returns the list of missing rows
     *
     * @return the missing rows
     */
    public List<String> getMissingRows() {
        return missingRows;
    }

    /**
     * Returns the list of corrupt rows
     *
     * @return the corrupt rows
     */
    public List<String> getCorruptRows() {
        return corruptRows;
    }

    @Override
    public void missingRow(final String sqlRow) {
        missingRows.add(sqlRow);
    }

    @Override
    public void corruptRow(final String row) {
        corruptRows.add(row);
    }

    @Override
    public String toString() {
        if (verified()) {
            return "VERIFICATION SUCCEEDED!";
        } else {
            return "VERIFICATION FAILED: " + missingRows.size() + " missing rows; "
                    + corruptRows.size() + " corrupt rows.";
        }
    }
}
